# coding=utf-8

"""
RSS 抓取器

负责从配置的 RSS 源抓取数据并转换为标准格式
"""

import time
import random
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .parser import RSSParser
from trendradar.storage.base import RSSItem, RSSData
from trendradar.utils.time import get_configured_time, DEFAULT_TIMEZONE


@dataclass
class RSSFeedConfig:
    """RSS 源配置"""

    id: str
    name: str
    url: str
    max_items: int = 0
    enabled: bool = True
    max_age_days: Optional[int] = None


class RSSFetcher:
    """RSS 抓取器"""

    def __init__(
        self,
        feeds: List[RSSFeedConfig],
        request_interval: int = 2000,
        timeout: int = 15,
        use_proxy: bool = False,
        proxy_url: str = "",
        timezone: str = DEFAULT_TIMEZONE,
        freshness_enabled: bool = True,
        default_max_age_days: int = 3,
    ):
        """
        初始化抓取器

        Args:
            feeds: RSS 源配置列表
            request_interval: 请求间隔（毫秒）
            timeout: 请求超时（秒）
            use_proxy: 是否使用代理
            proxy_url: 代理 URL
            timezone: 时区配置（如 'Asia/Shanghai'）
            freshness_enabled: 是否启用新鲜度过滤
            default_max_age_days: 默认最大文章年龄（天）
        """
        self.feeds = [f for f in feeds if f.enabled]
        self.request_interval = request_interval
        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.timezone = timezone
        self.freshness_enabled = freshness_enabled
        self.default_max_age_days = default_max_age_days

        self.parser = RSSParser()
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        创建请求会话。

        对 RSSHub 等公共 RSS 服务：
        - 使用常规浏览器 User-Agent；
        - 对 429 / 5xx 自动重试；
        - 尊重 Retry-After；
        - 支持代理。
        """

        session = requests.Session()

        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "application/rss+xml, "
                    "application/atom+xml, "
                    "application/xml;q=0.9, "
                    "text/xml;q=0.9, "
                    "*/*;q=0.8"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

        retry = Retry(
            total=4,
            connect=4,
            read=4,
            status=4,
            backoff_factor=2,
            status_forcelist=[
                429,
                500,
                502,
                503,
                504,
            ],
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        if self.use_proxy and self.proxy_url:
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url,
            }

        return session

    def fetch_feed(
        self,
        feed: RSSFeedConfig,
    ) -> Tuple[List[RSSItem], Optional[str]]:
        """
        抓取单个 RSS 源。

        Args:
            feed: RSS 源配置

        Returns:
            (条目列表, 错误信息)
        """

        try:
            response = self.session.get(
                feed.url,
                timeout=(10, self.timeout),
                allow_redirects=True,
            )

            response.raise_for_status()

            content_type = response.headers.get(
                "Content-Type",
                "",
            ).lower()

            content = response.text
            stripped = content.lstrip().lower()

            # RSS / Atom / JSON Feed 基本识别
            #
            # 注意：
            # 这里必须是 "<?xml"、"<rss"、"<feed"
            # 不能写成 "\<?xml"、"\<rss"。
            looks_like_feed = (
                stripped.startswith("<?xml")
                or stripped.startswith("<rss")
                or stripped.startswith("<feed")
                or stripped.startswith("{")
            )

            if not looks_like_feed:
                raise ValueError(
                    "返回内容不是 RSS/Atom/JSON Feed；"
                    f"status={response.status_code}, "
                    f"content_type={content_type}, "
                    f"final_url={response.url}, "
                    f"preview={content[:200]!r}"
                )

            parsed_items = self.parser.parse(
                content,
                feed.url,
            )

            # 限制条目数量
            # 0 = 不限制
            if feed.max_items > 0:
                parsed_items = parsed_items[: feed.max_items]

            now = get_configured_time(self.timezone)
            crawl_time = now.strftime("%H:%M")

            items: List[RSSItem] = []

            for parsed in parsed_items:
                item = RSSItem(
                    title=parsed.title,
                    feed_id=feed.id,
                    feed_name=feed.name,
                    url=parsed.url,
                    guid=parsed.guid or "",
                    published_at=parsed.published_at or "",
                    summary=parsed.summary or "",
                    author=parsed.author or "",
                    crawl_time=crawl_time,
                    first_time=crawl_time,
                    last_time=crawl_time,
                    count=1,
                )

                items.append(item)

            # 新鲜度过滤由 TrendRadar 后续阶段处理。
            # 抓取层尽量完整保存 RSS 数据。
            print(
                f"[RSS] {feed.name}: "
                f"获取 {len(items)} 条 "
                f"(status={response.status_code}, "
                f"content-type={content_type})"
            )

            return items, None

        except requests.Timeout:
            error = f"请求超时 ({self.timeout}s)"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

        except requests.RequestException as e:
            error = f"请求失败: {e}"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

        except ValueError as e:
            error = f"解析失败: {e}"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

        except Exception as e:
            error = f"未知错误: {type(e).__name__}: {e}"
            print(f"[RSS] {feed.name}: {error}")
            return [], error

    def fetch_all(self) -> RSSData:
        """
        抓取所有 RSS 源。

        Returns:
            RSSData 对象
        """

        all_items: Dict[str, List[RSSItem]] = {}
        id_to_name: Dict[str, str] = {}
        failed_ids: List[str] = []

        now = get_configured_time(self.timezone)
        crawl_time = now.strftime("%H:%M")
        crawl_date = now.strftime("%Y-%m-%d")

        print(
            f"[RSS] 开始抓取 {len(self.feeds)} 个 RSS 源..."
        )

        for i, feed in enumerate(self.feeds):
            # 请求间隔 + 少量随机抖动
            # 减少同一个 RSSHub 公共实例的瞬时压力
            if i > 0:
                interval = self.request_interval / 1000

                jitter = random.uniform(
                    -0.2,
                    0.2,
                ) * interval

                sleep_time = max(
                    0,
                    interval + jitter,
                )

                time.sleep(sleep_time)

            items, error = self.fetch_feed(feed)

            id_to_name[feed.id] = feed.name

            if error:
                failed_ids.append(feed.id)

            else:
                all_items[feed.id] = items

        total_items = sum(
            len(items)
            for items in all_items.values()
        )

        print(
            "[RSS] 抓取完成: "
            f"{len(all_items)} 个源成功, "
            f"{len(failed_ids)} 个失败, "
            f"共 {total_items} 条"
        )

        if failed_ids:
            print(
                "[RSS] 失败源 ID: "
                + ", ".join(failed_ids)
            )

        return RSSData(
            date=crawl_date,
            crawl_time=crawl_time,
            items=all_items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    @classmethod
    def from_config(
        cls,
        config: Dict,
    ) -> "RSSFetcher":
        """
        从配置字典创建抓取器。

        Args:
            config:
                {
                    "enabled": True,
                    "request_interval": 2000,
                    "timeout": 15,
                    "freshness_filter": {
                        "enabled": True,
                        "max_age_days": 3
                    },
                    "feeds": [
                        {
                            "id": "hacker-news",
                            "name": "Hacker News",
                            "url": "...",
                            "max_age_days": 1
                        }
                    ]
                }

        Returns:
            RSSFetcher 实例
        """

        freshness_config = config.get(
            "freshness_filter",
            {},
        )

        freshness_enabled = freshness_config.get(
            "enabled",
            True,
        )

        default_max_age_days = freshness_config.get(
            "max_age_days",
            3,
        )

        feeds: List[RSSFeedConfig] = []

        for feed_config in config.get(
            "feeds",
            [],
        ):
            max_age_days_raw = feed_config.get(
                "max_age_days"
            )

            max_age_days = None

            if max_age_days_raw is not None:
                try:
                    max_age_days = int(
                        max_age_days_raw
                    )

                    if max_age_days < 0:
                        feed_id = feed_config.get(
                            "id",
                            "unknown",
                        )

                        print(
                            "[警告] "
                            f"RSS feed '{feed_id}' "
                            "的 max_age_days 为负数，"
                            "将使用全局默认值"
                        )

                        max_age_days = None

                except (ValueError, TypeError):
                    feed_id = feed_config.get(
                        "id",
                        "unknown",
                    )

                    print(
                        "[警告] "
                        f"RSS feed '{feed_id}' "
                        "的 max_age_days 格式错误："
                        f"{max_age_days_raw}"
                    )

                    max_age_days = None

            feed = RSSFeedConfig(
                id=feed_config.get(
                    "id",
                    "",
                ),
                name=feed_config.get(
                    "name",
                    "",
                ),
                url=feed_config.get(
                    "url",
                    "",
                ),
                max_items=feed_config.get(
                    "max_items",
                    0,
                ),
                enabled=feed_config.get(
                    "enabled",
                    True,
                ),
                max_age_days=max_age_days,
            )

            if feed.id and feed.url:
                feeds.append(feed)

        return cls(
            feeds=feeds,
            request_interval=config.get(
                "request_interval",
                2000,
            ),
            timeout=config.get(
                "timeout",
                15,
            ),
            use_proxy=config.get(
                "use_proxy",
                False,
            ),
            proxy_url=config.get(
                "proxy_url",
                "",
            ),
            timezone=config.get(
                "timezone",
                DEFAULT_TIMEZONE,
            ),
            freshness_enabled=freshness_enabled,
            default_max_age_days=default_max_age_days,
        )
