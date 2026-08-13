# coding=utf-8
"""
远程存储后端（S3 兼容协议）

基于 TrendRadar v6.10.0 remote.py。

本定制版增加：
- RSS 跨日历史去重种子：
  当天 rss/YYYY-MM-DD.db 第一次建立时，从最近 7 天远程 RSS 数据库读取
  rss_items，并按 guid + feed_id（无 guid 时 url + feed_id）合并到当天库。
- 历史种子的 first_crawl_time / last_crawl_time 固定为 "00:00"，
  使 TrendRadar 原有 incremental 检测能把它们识别为“当前批次之前已经见过”。

不新增数据库表，不修改 rss_schema.sql / sqlite_mixin.py。
"""

import pytz
import re
import shutil
import sys
import tempfile
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    boto3 = None
    BotoConfig = None
    ClientError = Exception

from trendradar.storage.base import StorageBackend, NewsData, RSSItem, RSSData
from trendradar.storage.sqlite_mixin import SQLiteStorageMixin
from trendradar.utils.time import (
    DEFAULT_TIMEZONE,
    get_configured_time,
    format_date_folder,
    format_time_filename,
)


class RemoteStorageBackend(SQLiteStorageMixin, StorageBackend):
    """远程云存储后端（S3 兼容协议）"""

    RSS_CROSS_DAY_LOOKBACK_DAYS = 7

    def __init__(
        self,
        bucket_name: str,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: str,
        region: str = "",
        enable_txt: bool = False,
        enable_html: bool = True,
        temp_dir: Optional[str] = None,
        timezone: str = DEFAULT_TIMEZONE,
    ):
        if not HAS_BOTO3:
            raise ImportError("远程存储后端需要安装 boto3: pip install boto3")

        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.region = region
        self.enable_txt = enable_txt
        self.enable_html = enable_html
        self.timezone = timezone

        self.temp_dir = (
            Path(temp_dir)
            if temp_dir
            else Path(tempfile.mkdtemp(prefix="trendradar_"))
        )
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        use_sigv2 = (
            "myqcloud.com" in endpoint_url.lower()
            or "aliyuncs.com" in endpoint_url.lower()
        )
        signature_version = "s3" if use_sigv2 else "s3v4"

        s3_config = BotoConfig(
            s3={"addressing_style": "virtual"},
            signature_version=signature_version,
        )

        client_kwargs = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key_id,
            "aws_secret_access_key": secret_access_key,
            "config": s3_config,
        }
        if region:
            client_kwargs["region_name"] = region

        self.s3_client = boto3.client("s3", **client_kwargs)

        self._downloaded_files: List[Path] = []
        self._db_connections: Dict[str, sqlite3.Connection] = {}

        self._batch_mode = False
        self._batch_dirty: set = set()

        # 防止同一天多次重复执行历史播种。
        self._rss_history_seeded_dates: Set[str] = set()

        print(
            f"[远程存储] 初始化完成，存储桶: {bucket_name}，"
            f"签名版本: {signature_version}"
        )

    @property
    def backend_name(self) -> str:
        return "remote"

    @property
    def supports_txt(self) -> bool:
        return self.enable_txt

    # ========================================
    # SQLiteStorageMixin 抽象方法
    # ========================================

    def _get_configured_time(self) -> datetime:
        return get_configured_time(self.timezone)

    def _format_date_folder(self, date: Optional[str] = None) -> str:
        return format_date_folder(date, self.timezone)

    def _format_time_filename(self) -> str:
        return format_time_filename(self.timezone)

    def _get_remote_db_key(
        self,
        date: Optional[str] = None,
        db_type: str = "news",
    ) -> str:
        date_folder = self._format_date_folder(date)
        return f"{db_type}/{date_folder}.db"

    def _get_local_db_path(
        self,
        date: Optional[str] = None,
        db_type: str = "news",
    ) -> Path:
        date_folder = self._format_date_folder(date)
        db_dir = self.temp_dir / db_type
        db_dir.mkdir(parents=True, exist_ok=True)
        return db_dir / f"{date_folder}.db"

    def _check_object_exists(self, key: str) -> bool:
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "Not Found"):
                return False
            print(f"[远程存储] 检查对象存在性失败 ({key}): {e}")
            return False
        except Exception as e:
            print(f"[远程存储] 检查对象存在性异常 ({key}): {e}")
            return False

    def _download_sqlite(
        self,
        date: Optional[str] = None,
        db_type: str = "news",
    ) -> Optional[Path]:
        key = self._get_remote_db_key(date, db_type)
        local_path = self._get_local_db_path(date, db_type)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if not self._check_object_exists(key):
            print(f"[远程存储] 文件不存在，将创建新数据库: {key}")
            return None

        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            with open(local_path, "wb") as f:
                for chunk in response["Body"].iter_chunks(
                    chunk_size=1024 * 1024
                ):
                    f.write(chunk)

            self._downloaded_files.append(local_path)
            print(f"[远程存储] 已下载: {key} -> {local_path}")
            return local_path

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey", "Not Found"):
                print(f"[远程存储] 文件不存在，将创建新数据库: {key}")
                return None
            print(f"[远程存储] 下载失败 (错误码: {error_code}): {e}")
            raise
        except Exception as e:
            print(f"[远程存储] 下载异常: {e}")
            raise

    def _download_remote_db_to_path(
        self,
        key: str,
        local_path: Path,
    ) -> bool:
        """下载指定远程 DB 到独立临时路径，不建立业务连接。"""

        if not self._check_object_exists(key):
            return False

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key,
            )
            with open(local_path, "wb") as f:
                for chunk in response["Body"].iter_chunks(
                    chunk_size=1024 * 1024
                ):
                    f.write(chunk)
            return True
        except Exception as e:
            print(f"[RSS跨日去重] 下载历史库失败 {key}: {e}")
            return False

    def begin_batch(self):
        self._batch_mode = True
        self._batch_dirty.clear()

    def end_batch(self):
        self._batch_mode = False
        for date, db_type in self._batch_dirty:
            self._upload_sqlite(date, db_type)
        self._batch_dirty.clear()

    def _upload_sqlite(
        self,
        date: Optional[str] = None,
        db_type: str = "news",
    ) -> bool:
        if self._batch_mode:
            self._batch_dirty.add((date, db_type))
            return True

        local_path = self._get_local_db_path(date, db_type)
        key = self._get_remote_db_key(date, db_type)

        if not local_path.exists():
            print(f"[远程存储] 本地文件不存在，无法上传: {local_path}")
            return False

        try:
            local_size = local_path.stat().st_size
            print(
                f"[远程存储] 准备上传: {local_path} "
                f"({local_size} bytes) -> {key}"
            )

            with open(local_path, "rb") as f:
                file_content = f.read()

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=file_content,
                ContentLength=local_size,
                ContentType="application/x-sqlite3",
            )

            print(f"[远程存储] 已上传: {local_path} -> {key}")

            if self._check_object_exists(key):
                print(f"[远程存储] 上传验证成功: {key}")
                return True

            print("[远程存储] 上传验证失败: 文件未在远程存储中找到")
            return False

        except Exception as e:
            print(f"[远程存储] 上传失败: {e}")
            return False

    def _get_connection(
        self,
        date: Optional[str] = None,
        db_type: str = "news",
    ) -> sqlite3.Connection:
        local_path = self._get_local_db_path(date, db_type)
        db_path = str(local_path)

        if db_path not in self._db_connections:
            local_path.parent.mkdir(parents=True, exist_ok=True)

            if not local_path.exists():
                self._download_sqlite(date, db_type)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            self._init_tables(conn, db_type)
            self._db_connections[db_path] = conn

        return self._db_connections[db_path]

    # ========================================
    # RSS 跨日去重
    # ========================================

    @staticmethod
    def _rss_history_identity(
        feed_id: str,
        guid: str,
        url: str,
    ) -> Optional[Tuple[str, str, str]]:
        """
        与 TrendRadar v6.10.0 一致：
        guid 优先；guid 为空时使用 url。
        """
        guid = (guid or "").strip()
        url = (url or "").strip()

        if guid:
            return ("guid", feed_id, guid)
        if url:
            return ("url", feed_id, url)
        return None

    def _seed_rss_history_from_previous_days(
        self,
        date: str,
        lookback_days: int = RSS_CROSS_DAY_LOOKBACK_DAYS,
    ) -> int:
        """
        当天 RSS DB 第一次使用时，把最近 N 天历史 rss_items 合并到当天库。

        注意：
        - 只复制 rss_feeds + rss_items。
        - 不复制 rss_crawl_records / rss_push_records。
        - 历史记录 first_crawl_time/last_crawl_time 写为 00:00，
          这样原有 incremental 检测会把它们视为当前批次之前的历史。
        """

        date_str = self._format_date_folder(date)

        if date_str in self._rss_history_seeded_dates:
            return 0

        self._rss_history_seeded_dates.add(date_str)

        conn = self._get_connection(date_str, db_type="rss")
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM rss_items")
        existing_count = cursor.fetchone()[0]

        # 当天库已有条目，说明已经运行过，不重复播种。
        if existing_count > 0:
            print(
                f"[RSS跨日去重] 当天库已有 {existing_count} 条记录，"
                "跳过历史播种"
            )
            return 0

        try:
            current_date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print(f"[RSS跨日去重] 日期格式异常: {date_str}")
            return 0

        seen: Set[Tuple[str, str, str]] = set()
        seed_rows = []
        feed_names: Dict[str, str] = {}

        for offset in range(1, lookback_days + 1):
            hist_date = (
                current_date - timedelta(days=offset)
            ).strftime("%Y-%m-%d")

            key = f"rss/{hist_date}.db"

            if not self._check_object_exists(key):
                continue

            history_path = (
                self.temp_dir
                / "_rss_history"
                / f"{hist_date}.db"
            )

            if not self._download_remote_db_to_path(
                key,
                history_path,
            ):
                continue

            hist_conn = None

            try:
                hist_conn = sqlite3.connect(
                    f"file:{history_path}?mode=ro",
                    uri=True,
                )
                hist_conn.row_factory = sqlite3.Row

                # 兼容早期数据库：先检查 guid 列。
                columns = {
                    row[1]
                    for row in hist_conn.execute(
                        "PRAGMA table_info(rss_items)"
                    ).fetchall()
                }
                has_guid = "guid" in columns

                if has_guid:
                    sql = """
                        SELECT
                            i.title,
                            i.feed_id,
                            COALESCE(f.name, i.feed_id) AS feed_name,
                            i.url,
                            COALESCE(i.guid, '') AS guid,
                            i.published_at,
                            i.summary,
                            i.author,
                            i.crawl_count,
                            i.created_at,
                            i.updated_at
                        FROM rss_items i
                        LEFT JOIN rss_feeds f
                            ON i.feed_id = f.id
                    """
                else:
                    sql = """
                        SELECT
                            i.title,
                            i.feed_id,
                            COALESCE(f.name, i.feed_id) AS feed_name,
                            i.url,
                            '' AS guid,
                            i.published_at,
                            i.summary,
                            i.author,
                            i.crawl_count,
                            i.created_at,
                            i.updated_at
                        FROM rss_items i
                        LEFT JOIN rss_feeds f
                            ON i.feed_id = f.id
                    """

                day_count = 0

                for row in hist_conn.execute(sql):
                    identity = self._rss_history_identity(
                        row["feed_id"],
                        row["guid"],
                        row["url"],
                    )
                    if identity is None or identity in seen:
                        continue

                    seen.add(identity)
                    day_count += 1

                    feed_names[row["feed_id"]] = (
                        row["feed_name"] or row["feed_id"]
                    )

                    seed_rows.append(
                        (
                            row["title"],
                            row["feed_id"],
                            row["url"] or "",
                            row["guid"] or "",
                            row["published_at"] or "",
                            row["summary"] or "",
                            row["author"] or "",
                            "00:00",
                            "00:00",
                            max(int(row["crawl_count"] or 1), 1),
                            row["created_at"]
                            or f"{hist_date} 00:00:00",
                            row["updated_at"]
                            or f"{hist_date} 00:00:00",
                        )
                    )

                print(
                    f"[RSS跨日去重] 读取历史库 {key}: "
                    f"{day_count} 条可用历史身份"
                )

            except sqlite3.Error as e:
                print(
                    f"[RSS跨日去重] 读取历史库失败 {key}: {e}"
                )

            finally:
                if hist_conn is not None:
                    hist_conn.close()

                try:
                    if history_path.exists():
                        history_path.unlink()
                except OSError:
                    pass

        if not seed_rows:
            print(
                f"[RSS跨日去重] 最近 {lookback_days} 天无可用历史 RSS 数据"
            )
            return 0

        now_str = self._get_configured_time().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        for feed_id, feed_name in feed_names.items():
            cursor.execute(
                """
                INSERT INTO rss_feeds (id, name, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    updated_at = excluded.updated_at
                """,
                (feed_id, feed_name, now_str),
            )

        inserted = 0

        for row in seed_rows:
            try:
                cursor.execute(
                    """
                    INSERT INTO rss_items
                    (
                        title, feed_id, url, guid,
                        published_at, summary, author,
                        first_crawl_time, last_crawl_time,
                        crawl_count, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # 同一身份可能由 URL / GUID 双索引与其他历史天重叠。
                pass

        conn.commit()

        print(
            f"[RSS跨日去重] 已将最近 {lookback_days} 天 "
            f"{inserted} 条历史 RSS 身份播种到 {date_str} 当天库"
        )

        return inserted

    # ========================================
    # StorageBackend：新闻
    # ========================================

    def save_news_data(self, data: NewsData) -> bool:
        conn = self._get_connection(data.date)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM news_items")
        row = cursor.fetchone()
        existing_count = row[0] if row else 0

        if existing_count > 0:
            print(
                f"[远程存储] 已有 {existing_count} 条历史记录，"
                "将合并新数据"
            )

        (
            success,
            new_count,
            updated_count,
            title_changed_count,
            off_list_count,
        ) = self._save_news_data_impl(
            data,
            "[远程存储]",
        )

        if not success:
            return False

        cursor.execute("SELECT COUNT(*) FROM news_items")
        row = cursor.fetchone()
        final_count = row[0] if row else 0

        log_parts = [
            f"[远程存储] 处理完成：新增 {new_count} 条"
        ]

        if updated_count > 0:
            log_parts.append(f"更新 {updated_count} 条")
        if title_changed_count > 0:
            log_parts.append(f"标题变更 {title_changed_count} 条")
        if off_list_count > 0:
            log_parts.append(f"脱榜 {off_list_count} 条")

        log_parts.append(f"(去重后总计: {final_count} 条)")
        print("，".join(log_parts))

        if self._upload_sqlite(data.date):
            print("[远程存储] 数据已同步到远程存储")
            return True

        print("[远程存储] 上传远程存储失败")
        return False

    def get_today_all_data(
        self,
        date: Optional[str] = None,
    ) -> Optional[NewsData]:
        return self._get_today_all_data_impl(date)

    def get_latest_crawl_data(
        self,
        date: Optional[str] = None,
    ) -> Optional[NewsData]:
        return self._get_latest_crawl_data_impl(date)

    def detect_new_titles(
        self,
        current_data: NewsData,
    ) -> Dict[str, Dict]:
        return self._detect_new_titles_impl(current_data)

    def is_first_crawl_today(
        self,
        date: Optional[str] = None,
    ) -> bool:
        return self._is_first_crawl_today_impl(date)

    # ========================================
    # 时间段执行记录
    # ========================================

    def has_period_executed(
        self,
        date_str: str,
        period_key: str,
        action: str,
    ) -> bool:
        return self._has_period_executed_impl(
            date_str,
            period_key,
            action,
        )

    def record_period_execution(
        self,
        date_str: str,
        period_key: str,
        action: str,
    ) -> bool:
        success = self._record_period_execution_impl(
            date_str,
            period_key,
            action,
        )

        if not success:
            return False

        now_str = self._get_configured_time().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        print(
            f"[远程存储] 时间段执行记录已保存: "
            f"{period_key}/{action} at {now_str}"
        )

        if self._upload_sqlite(date_str):
            print("[远程存储] 时间段执行记录已同步到远程存储")
            return True

        print("[远程存储] 时间段执行记录同步到远程存储失败")
        return False

    # ========================================
    # RSS
    # ========================================

    def save_rss_data(self, data: RSSData) -> bool:
        # 关键定制：在 _save_rss_data_impl 前注入跨日历史身份。
        self._seed_rss_history_from_previous_days(
            data.date,
            lookback_days=self.RSS_CROSS_DAY_LOOKBACK_DAYS,
        )

        success, new_count, updated_count = (
            self._save_rss_data_impl(
                data,
                "[远程存储]",
            )
        )

        if not success:
            return False

        log_parts = [
            f"[远程存储] RSS 处理完成：新增 {new_count} 条"
        ]
        if updated_count > 0:
            log_parts.append(f"更新 {updated_count} 条")

        print("，".join(log_parts))

        if self._upload_sqlite(
            data.date,
            db_type="rss",
        ):
            print("[远程存储] RSS 数据已同步到远程存储")
            return True

        print("[远程存储] RSS 上传远程存储失败")
        return False

    def get_rss_data(
        self,
        date: Optional[str] = None,
    ) -> Optional[RSSData]:
        return self._get_rss_data_impl(date)

    def detect_new_rss_items(
        self,
        current_data: RSSData,
    ) -> Dict[str, List[RSSItem]]:
        return self._detect_new_rss_items_impl(current_data)

    def get_latest_rss_data(
        self,
        date: Optional[str] = None,
    ) -> Optional[RSSData]:
        return self._get_latest_rss_data_impl(date)

    # ========================================
    # AI 智能筛选
    # ========================================

    def get_active_ai_filter_tags(
        self,
        date=None,
        interests_file="ai_interests.txt",
    ):
        return self._get_active_tags_impl(
            date,
            interests_file,
        )

    def get_latest_prompt_hash(
        self,
        date=None,
        interests_file="ai_interests.txt",
    ):
        return self._get_latest_prompt_hash_impl(
            date,
            interests_file,
        )

    def get_latest_ai_filter_tag_version(self, date=None):
        return self._get_latest_tag_version_impl(date)

    def deprecate_all_ai_filter_tags(
        self,
        date=None,
        interests_file="ai_interests.txt",
    ):
        count = self._deprecate_all_tags_impl(
            date,
            interests_file,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def save_ai_filter_tags(
        self,
        tags,
        version,
        prompt_hash,
        date=None,
        interests_file="ai_interests.txt",
    ):
        count = self._save_tags_impl(
            date,
            tags,
            version,
            prompt_hash,
            interests_file,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def save_ai_filter_results(
        self,
        results,
        date=None,
    ):
        count = self._save_filter_results_impl(
            date,
            results,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def get_active_ai_filter_results(
        self,
        date=None,
        interests_file="ai_interests.txt",
    ):
        return self._get_active_filter_results_impl(
            date,
            interests_file,
        )

    def deprecate_specific_ai_filter_tags(
        self,
        tag_ids,
        date=None,
    ):
        count = self._deprecate_specific_tags_impl(
            date,
            tag_ids,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def update_ai_filter_tags_hash(
        self,
        interests_file,
        new_hash,
        date=None,
    ):
        count = self._update_tags_hash_impl(
            date,
            interests_file,
            new_hash,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def update_ai_filter_tag_descriptions(
        self,
        tag_updates,
        date=None,
        interests_file="ai_interests.txt",
    ):
        count = self._update_tag_descriptions_impl(
            date,
            tag_updates,
            interests_file,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def update_ai_filter_tag_priorities(
        self,
        tag_priorities,
        date=None,
        interests_file="ai_interests.txt",
    ):
        count = self._update_tag_priorities_impl(
            date,
            tag_priorities,
            interests_file,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def save_analyzed_news(
        self,
        news_ids,
        source_type,
        interests_file,
        prompt_hash,
        matched_ids,
        date=None,
    ):
        count = self._save_analyzed_news_impl(
            date,
            news_ids,
            source_type,
            interests_file,
            prompt_hash,
            matched_ids,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def get_analyzed_news_ids(
        self,
        source_type="hotlist",
        date=None,
        interests_file="ai_interests.txt",
    ):
        return self._get_analyzed_news_ids_impl(
            date,
            source_type,
            interests_file,
        )

    def clear_analyzed_news(
        self,
        date=None,
        interests_file="ai_interests.txt",
    ):
        count = self._clear_analyzed_news_impl(
            date,
            interests_file,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def clear_unmatched_analyzed_news(
        self,
        date=None,
        interests_file="ai_interests.txt",
    ):
        count = self._clear_unmatched_analyzed_news_impl(
            date,
            interests_file,
        )
        if count > 0:
            self._upload_sqlite(date)
        return count

    def get_all_news_ids(self, date=None):
        return self._get_all_news_ids_impl(date)

    def get_all_rss_ids(self, date=None):
        return self._get_all_rss_ids_impl(date)

    # ========================================
    # TXT / HTML
    # ========================================

    def save_txt_snapshot(
        self,
        data: NewsData,
    ) -> Optional[str]:
        if not self.enable_txt:
            return None

        try:
            date_folder = self._format_date_folder(data.date)
            txt_dir = self.temp_dir / date_folder / "txt"
            txt_dir.mkdir(parents=True, exist_ok=True)

            file_path = txt_dir / f"{data.crawl_time}.txt"

            with open(
                file_path,
                "w",
                encoding="utf-8",
            ) as f:
                for source_id, news_list in data.items.items():
                    source_name = data.id_to_name.get(
                        source_id,
                        source_id,
                    )

                    if source_name and source_name != source_id:
                        f.write(
                            f"{source_id} | {source_name}\n"
                        )
                    else:
                        f.write(f"{source_id}\n")

                    sorted_news = sorted(
                        news_list,
                        key=lambda x: x.rank,
                    )

                    for item in sorted_news:
                        line = f"{item.rank}. {item.title}"

                        if item.url:
                            line += f" [URL:{item.url}]"
                        if item.mobile_url:
                            line += (
                                f" [MOBILE:{item.mobile_url}]"
                            )

                        f.write(line + "\n")

                    f.write("\n")

                if data.failed_ids:
                    f.write(
                        "==== 以下ID请求失败 ====\n"
                    )
                    for failed_id in data.failed_ids:
                        f.write(f"{failed_id}\n")

            print(
                f"[远程存储] TXT 快照已保存: {file_path}"
            )
            return str(file_path)

        except Exception as e:
            print(f"[远程存储] 保存 TXT 快照失败: {e}")
            return None

    def save_html_report(
        self,
        html_content: str,
        filename: str,
    ) -> Optional[str]:
        if not self.enable_html:
            return None

        try:
            date_folder = self._format_date_folder()
            html_dir = self.temp_dir / date_folder / "html"
            html_dir.mkdir(parents=True, exist_ok=True)

            file_path = html_dir / filename

            with open(
                file_path,
                "w",
                encoding="utf-8",
            ) as f:
                f.write(html_content)

            print(
                f"[远程存储] HTML 报告已保存: {file_path}"
            )
            return str(file_path)

        except Exception as e:
            print(f"[远程存储] 保存 HTML 报告失败: {e}")
            return None

    # ========================================
    # 清理
    # ========================================

    def cleanup(self) -> None:
        if sys.meta_path is None:
            return

        db_connections = getattr(
            self,
            "_db_connections",
            {},
        )

        for db_path, conn in list(db_connections.items()):
            try:
                conn.close()
                print(
                    f"[远程存储] 关闭数据库连接: {db_path}"
                )
            except Exception as e:
                print(
                    f"[远程存储] 关闭连接失败 {db_path}: {e}"
                )

        if db_connections:
            db_connections.clear()

        temp_dir = getattr(
            self,
            "temp_dir",
            None,
        )

        if temp_dir:
            try:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    print(
                        f"[远程存储] 临时目录已清理: {temp_dir}"
                    )
            except Exception as e:
                if sys.meta_path is not None:
                    print(
                        f"[远程存储] 清理临时目录失败: {e}"
                    )

        downloaded_files = getattr(
            self,
            "_downloaded_files",
            None,
        )
        if downloaded_files:
            downloaded_files.clear()

    def cleanup_old_data(
        self,
        retention_days: int,
    ) -> int:
        if retention_days <= 0:
            return 0

        deleted_count = 0
        cutoff_date = (
            self._get_configured_time()
            - timedelta(days=retention_days)
        )

        try:
            paginator = self.s3_client.get_paginator(
                "list_objects_v2"
            )
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix="news/",
            )

            objects_to_delete = []
            deleted_dates = set()

            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]

                    try:
                        date_match = re.match(
                            r"news/(\d{4})-(\d{2})-(\d{2})\.db$",
                            key,
                        )
                        if not date_match:
                            continue

                        folder_date = datetime(
                            int(date_match.group(1)),
                            int(date_match.group(2)),
                            int(date_match.group(3)),
                            tzinfo=pytz.timezone(
                                self.timezone
                            ),
                        )
                        date_str = (
                            f"{date_match.group(1)}-"
                            f"{date_match.group(2)}-"
                            f"{date_match.group(3)}"
                        )
                    except Exception:
                        continue

                    if folder_date < cutoff_date:
                        objects_to_delete.append(
                            {"Key": key}
                        )
                        deleted_dates.add(date_str)

            if objects_to_delete:
                batch_size = 1000

                for i in range(
                    0,
                    len(objects_to_delete),
                    batch_size,
                ):
                    batch = objects_to_delete[
                        i : i + batch_size
                    ]

                    try:
                        self.s3_client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={"Objects": batch},
                        )
                        print(
                            f"[远程存储] 删除 {len(batch)} 个对象"
                        )
                    except Exception as e:
                        print(
                            f"[远程存储] 批量删除失败: {e}"
                        )

                deleted_count = len(deleted_dates)

                for date_str in sorted(deleted_dates):
                    print(
                        "[远程存储] 清理过期数据: "
                        f"news/{date_str}.db"
                    )

                print(
                    "[远程存储] 共清理 "
                    f"{deleted_count} 个过期日期数据库文件"
                )

            return deleted_count

        except Exception as e:
            print(f"[远程存储] 清理过期数据失败: {e}")
            return deleted_count

    def __del__(self):
        if sys.meta_path is None:
            return

        try:
            self.cleanup()
        except Exception:
            pass

    # ========================================
    # 数据拉取 / 日期列表
    # ========================================

    def pull_recent_days(
        self,
        days: int,
        local_data_dir: str = "output",
    ) -> int:
        if days <= 0:
            return 0

        local_dir = Path(local_data_dir)
        local_dir.mkdir(parents=True, exist_ok=True)

        pulled_count = 0
        now = self._get_configured_time()

        print(
            f"[远程存储] 开始拉取最近 {days} 天的数据..."
        )

        for i in range(days):
            date = now - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")

            local_date_dir = local_dir / date_str
            local_db_path = local_date_dir / "news.db"

            if local_db_path.exists():
                print(
                    f"[远程存储] 跳过（本地已存在）: {date_str}"
                )
                continue

            remote_key = f"news/{date_str}.db"

            if not self._check_object_exists(remote_key):
                print(
                    f"[远程存储] 跳过（远程不存在）: {date_str}"
                )
                continue

            try:
                local_date_dir.mkdir(
                    parents=True,
                    exist_ok=True,
                )
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=remote_key,
                )

                with open(
                    local_db_path,
                    "wb",
                ) as f:
                    for chunk in response[
                        "Body"
                    ].iter_chunks(
                        chunk_size=1024 * 1024
                    ):
                        f.write(chunk)

                print(
                    f"[远程存储] 已拉取: "
                    f"{remote_key} -> {local_db_path}"
                )
                pulled_count += 1

            except Exception as e:
                print(
                    f"[远程存储] 拉取失败 ({date_str}): {e}"
                )

        print(
            f"[远程存储] 拉取完成，共下载 "
            f"{pulled_count} 个数据库文件"
        )

        return pulled_count

    def list_remote_dates(self) -> List[str]:
        dates = []

        try:
            paginator = self.s3_client.get_paginator(
                "list_objects_v2"
            )
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix="news/",
            )

            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    date_match = re.match(
                        r"news/(\d{4}-\d{2}-\d{2})\.db$",
                        key,
                    )
                    if date_match:
                        dates.append(date_match.group(1))

            return sorted(
                dates,
                reverse=True,
            )

        except Exception as e:
            print(
                f"[远程存储] 列出远程日期失败: {e}"
            )
            return []
