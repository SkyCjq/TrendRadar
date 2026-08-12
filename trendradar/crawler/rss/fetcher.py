# coding=utf-8

"""
RSS 抓取器

负责从配置的 RSS 源抓取数据并转换为标准格式。

扩展能力：
1. allowed_authors
   对 RSSHub keyword 结果做官方作者精确白名单过滤。

2. ticket_enrich
   对篮球票务相关微博中的外链进行跟随，
   抓取真实票务项目页面，并把票价、开售时间、
   实名、限购、退改、销售状态等证据追加到 RSSItem.summary。
"""

import html
import json
import random
import re
import time

from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .parser import RSSParser
from trendradar.storage.base import RSSItem, RSSData
from trendradar.utils.time import (
    get_configured_time,
    DEFAULT_TIMEZONE,
)


@dataclass
class RSSFeedConfig:
    """RSS 源配置"""

    id: str
    name: str
    url: str
    max_items: int = 0
    enabled: bool = True
    max_age_days: Optional[int] = None

    # keyword RSS 官方作者精确白名单
    allowed_authors: List[str] = field(
        default_factory=list
    )

    # 是否抓取微博中的真实票务项目外链
    ticket_enrich: bool = False


class _VisibleTextParser(HTMLParser):
    """从 HTML 中提取可见文本"""

    def __init__(self):
        super().__init__(
            convert_charrefs=True
        )

        self._skip_depth = 0
        self._parts: List[str] = []

    def handle_starttag(
        self,
        tag,
        attrs,
    ):
        if tag.lower() in {
            "script",
            "style",
            "noscript",
            "svg",
        }:
            self._skip_depth += 1

    def handle_endtag(
        self,
        tag,
    ):
        if (
            tag.lower()
            in {
                "script",
                "style",
                "noscript",
                "svg",
            }
            and self._skip_depth > 0
        ):
            self._skip_depth -= 1

    def handle_data(
        self,
        data,
    ):
        if self._skip_depth == 0:
            text = re.sub(
                r"\s+",
                " ",
                data,
            ).strip()

            if text:
                self._parts.append(text)

    def get_text(
        self,
    ) -> str:
        text = " ".join(
            self._parts
        )

        return re.sub(
            r"\s+",
            " ",
            text,
        ).strip()


class RSSFetcher:
    """RSS 抓取器"""

    RSSHUB_FALLBACK_FROM = (
        "https://rsshub.app/"
    )

    RSSHUB_FALLBACK_TO = (
        "https://rsshub.rss3.workers.dev/"
    )

    TICKET_WORDS = (
        "售票",
        "购票",
        "门票",
        "开票",
        "票价",
        "票档",
        "余票",
        "售罄",
        "抢票",
        "预售",
        "实名",
        "退票",
        "退款",
        "限购",
        "票星球",
        "大麦",
        "看个比赛",
        "猫眼",
    )

    BASKETBALL_WORDS = (
        "篮球",
        "CBA",
        "WCBA",
        "NBL",
        "男篮",
        "女篮",
        "中国队",
        "全明星",
        "俱乐部杯",
    )

    SKIP_LINK_HOST_KEYWORDS = (
        "weibo.com",
        "m.weibo.cn",
        "video.weibo.com",
        "weibocdn.com",
        "sinaimg.cn",
        "rsshub.",
    )

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
        self.feeds = [
            f
            for f in feeds
            if f.enabled
        ]

        self.request_interval = (
            request_interval
        )

        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.timezone = timezone

        self.freshness_enabled = (
            freshness_enabled
        )

        self.default_max_age_days = (
            default_max_age_days
        )

        self.parser = RSSParser()

        self.session = (
            self._create_session()
        )

    def _create_session(
        self,
    ) -> requests.Session:
        """创建请求会话"""

        session = requests.Session()

        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/124.0.0.0 "
                    "Safari/537.36"
                ),
                "Accept": (
                    "application/rss+xml, "
                    "application/atom+xml, "
                    "application/xml;q=0.9, "
                    "text/xml;q=0.9, "
                    "text/html;q=0.8, "
                    "*/*;q=0.7"
                ),
                "Accept-Language": (
                    "zh-CN,zh;q=0.9,"
                    "en;q=0.8"
                ),
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
            allowed_methods=frozenset(
                [
                    "GET",
                    "HEAD",
                ]
            ),
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
        )

        session.mount(
            "http://",
            adapter,
        )

        session.mount(
            "https://",
            adapter,
        )

        if (
            self.use_proxy
            and self.proxy_url
        ):
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url,
            }

        return session

    @staticmethod
    def _normalize_author(
        author: str,
    ) -> str:
        return re.sub(
            r"\s+",
            "",
            (
                author
                or ""
            ).strip(),
        ).lower()

    def _author_allowed(
        self,
        feed: RSSFeedConfig,
        author: str,
    ) -> bool:
        if not feed.allowed_authors:
            return True

        normalized = (
            self._normalize_author(
                author
            )
        )

        allowed = {
            self._normalize_author(
                item
            )
            for item
            in feed.allowed_authors
            if item
        }

        return normalized in allowed

    @staticmethod
    def _extract_links(
        fragment: str,
    ) -> List[str]:
        """
        从 RSS description / summary
        中提取 HTTP 外链
        """

        if not fragment:
            return []

        decoded = html.unescape(
            fragment
        )

        links: List[str] = []

        for match in re.findall(
            r'href\s*=\s*["\']([^"\']+)["\']',
            decoded,
            flags=re.IGNORECASE,
        ):
            links.append(
                match.strip()
            )

        for match in re.findall(
            r'https?://[^\s<>"\']+',
            decoded,
            flags=re.IGNORECASE,
        ):
            links.append(
                match.rstrip(
                    ".,;，。；）)]}"
                )
            )

        result: List[str] = []
        seen = set()

        for url in links:
            if not url.startswith(
                (
                    "http://",
                    "https://",
                )
            ):
                continue

            if url in seen:
                continue

            seen.add(url)
            result.append(url)

        return result

    def _is_ticket_context(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
    ) -> bool:
        text = (
            f"{feed.name} "
            f"{title} "
            f"{summary}"
        )

        text_lower = text.lower()

        has_ticket = any(
            word.lower()
            in text_lower
            for word
            in self.TICKET_WORDS
        )

        has_basketball = any(
            word.lower()
            in text_lower
            for word
            in self.BASKETBALL_WORDS
        )

        # 篮协、联盟、俱乐部：
        # 只要明确出现购票类词即可抓。
        if feed.id.startswith(
            (
                "cba-",
                "china-basketball",
                "wcba-",
                "nbl-",
            )
        ):
            return has_ticket

        # 票务平台：
        # 必须同时满足票务 + 篮球，
        # 防止演唱会等内容进入项目页抓取。
        if feed.id.startswith(
            "ticket-"
        ):
            return (
                has_ticket
                and has_basketball
            )

        return False

    def _should_skip_final_url(
        self,
        url: str,
    ) -> bool:
        host = (
            urlparse(url).hostname
            or ""
        ).lower()

        return any(
            keyword in host
            for keyword
            in self.SKIP_LINK_HOST_KEYWORDS
        )

    @staticmethod
    def _extract_visible_text(
        content: str,
        limit: int = 5000,
    ) -> str:
        parser = _VisibleTextParser()

        try:
            parser.feed(content)

            text = (
                parser.get_text()
            )

        except Exception:
            text = re.sub(
                r"<[^>]+>",
                " ",
                content,
            )

            text = html.unescape(
                text
            )

            text = re.sub(
                r"\s+",
                " ",
                text,
            ).strip()

        return text[:limit]

    @staticmethod
    def _json_string(
        content: str,
        key: str,
    ) -> Optional[str]:
        pattern = (
            rf'"{re.escape(key)}"'
            r'\s*:\s*'
            r'"((?:\\.|[^"\\])*)"'
        )

        match = re.search(
            pattern,
            content,
        )

        if not match:
            return None

        raw = match.group(1)

        try:
            return json.loads(
                f'"{raw}"'
            )

        except Exception:
            return raw.replace(
                r"\/",
                "/",
            )

    @staticmethod
    def _json_number(
        content: str,
        key: str,
    ) -> Optional[str]:
        match = re.search(
            (
                rf'"{re.escape(key)}"'
                r"\s*:\s*"
                r"(-?\d+(?:\.\d+)?)"
            ),
            content,
        )

        if match:
            return match.group(1)

        return None

    @staticmethod
    def _json_bool(
        content: str,
        key: str,
    ) -> Optional[bool]:
        match = re.search(
            (
                rf'"{re.escape(key)}"'
                r"\s*:\s*"
                r"(true|false)"
            ),
            content,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        return (
            match.group(1).lower()
            == "true"
        )

    def _extract_damai_evidence(
        self,
        content: str,
        visible_text: str,
    ) -> str:
        """
        从真实大麦项目详情页提取字段
        """

        parts: List[str] = []

        field_map = (
            (
                "项目名",
                (
                    "projectName",
                    "itemName",
                ),
            ),
            (
                "票价区间",
                (
                    "priceRange",
                ),
            ),
            (
                "开售时间",
                (
                    "sellStartTimeStr",
                ),
            ),
            (
                "销售状态",
                (
                    "buyBtnText",
                ),
            ),
            (
                "比赛/演出日期",
                (
                    "performStartDate",
                ),
            ),
            (
                "场次",
                (
                    "performTime",
                    "performTimeDetail",
                ),
            ),
            (
                "城市",
                (
                    "cityName",
                ),
            ),
            (
                "场馆",
                (
                    "venueName",
                ),
            ),
        )

        for label, keys in field_map:
            value = None

            for key in keys:
                value = (
                    self._json_string(
                        content,
                        key,
                    )
                )

                if value:
                    break

            if value:
                parts.append(
                    f"{label}={value}"
                )

        single_limit = (
            self._json_number(
                content,
                "singleLimit",
            )
        )

        if single_limit:
            parts.append(
                f"单场限购="
                f"{single_limit}张"
            )

        real_name = (
            self._json_bool(
                content,
                "needRealNameCertified",
            )
        )

        if real_name is not None:
            if real_name:
                value = (
                    "需要实名/实人认证"
                )
            else:
                value = (
                    "页面结构字段显示"
                    "无需实名认证"
                )

            parts.append(
                f"实名要求={value}"
            )

        refund_label = None

        if (
            "不支持退换"
            in visible_text
            or "不支持退"
            in visible_text
        ):
            refund_label = (
                "不支持退/退换"
            )

        elif (
            "有条件退款"
            in visible_text
            or "条件退"
            in visible_text
        ):
            refund_label = (
                "支持有条件退款"
            )

        elif (
            "支持退款"
            in visible_text
        ):
            refund_label = (
                "支持退款"
            )

        if refund_label:
            parts.append(
                f"退改={refund_label}"
            )

        if not single_limit:
            limit_match = re.search(
                (
                    r"("
                    r"每笔订单最多购买\s*\d+\s*张"
                    r"|"
                    r"每个账号最多购买\s*\d+\s*张"
                    r")"
                ),
                visible_text,
            )

            if limit_match:
                parts.append(
                    "限购="
                    + limit_match.group(1)
                )

        return "；".join(parts)

    def _fetch_ticket_page(
        self,
        source_url: str,
    ) -> Optional[str]:
        """
        跟随短链 / 外链，
        抓真实票务项目页面
        """

        try:
            response = self.session.get(
                source_url,
                timeout=(
                    10,
                    min(
                        max(
                            self.timeout,
                            20,
                        ),
                        45,
                    ),
                ),
                allow_redirects=True,
            )

            response.raise_for_status()

        except requests.RequestException as exc:
            print(
                "[票务页] 外链抓取失败: "
                f"{source_url} -> {exc}"
            )

            return None

        final_url = response.url

        if self._should_skip_final_url(
            final_url
        ):
            return None

        content_type = (
            response.headers.get(
                "Content-Type",
                "",
            ).lower()
        )

        allowed_types = (
            "text/html",
            "application/xhtml",
            "text/plain",
            "application/json",
        )

        if not any(
            marker in content_type
            for marker
            in allowed_types
        ):
            return None

        # 防止异常大页面占用过多内存
        raw_content = (
            response.text[
                :2_000_000
            ]
        )

        visible_text = (
            self._extract_visible_text(
                raw_content,
                limit=5000,
            )
        )

        host = (
            urlparse(
                final_url
            ).hostname
            or ""
        ).lower()

        platform = (
            "公开票务项目页"
        )

        if "damai" in host:
            platform = "大麦"

        elif "maoyan" in host:
            platform = "猫眼"

        elif (
            "piaoxingqiu"
            in host
            or "票星球"
            in visible_text[:500]
        ):
            platform = "票星球"

        elif (
            "看个比赛"
            in visible_text[:500]
        ):
            platform = (
                "看个比赛"
            )

        structured = ""

        if "damai" in host:
            structured = (
                self._extract_damai_evidence(
                    raw_content,
                    visible_text,
                )
            )

        evidence_parts = [
            f"平台={platform}",
            f"最终项目链接={final_url}",
        ]

        if structured:
            evidence_parts.append(
                structured
            )

        if visible_text:
            evidence_parts.append(
                "页面正文="
                + visible_text[:3500]
            )

        else:
            evidence_parts.append(
                "页面正文不足；"
                "该页面可能依赖 "
                "JavaScript、App "
                "或登录态渲染"
            )

        print(
            "[票务页] 抓取成功: "
            f"{platform} -> {final_url}"
        )

        return "；".join(
            evidence_parts
        )

    def _enrich_ticket_summary(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
        raw_summary: str = "",
    ) -> str:
        """
        票务内容增强。

        summary:
            TrendRadar 已清洗的纯文本摘要，
            用于判断是否属于票务内容。

        raw_summary:
            RSS 原始 HTML description，
            用于寻找 href / t.cn / 项目页链接。

        返回值：
            始终保持纯文本 summary 为主体，
            只有抓到真实票务项目页时才追加证据。
        """

        if not feed.ticket_enrich:
            return summary

        # ------------------------------------------------------
        # Step 1：判断是不是需要票务增强的内容
        # ------------------------------------------------------
        if not self._is_ticket_context(
            feed,
            title,
            summary,
        ):
            return summary

        print(
            f"[票务检测] {feed.name}: "
            f"命中票务内容 -> "
            f"{title[:80]}"
        )

        # ------------------------------------------------------
        # Step 2：
        # 必须从 raw_summary 提取链接。
        #
        # parsed.summary 中 HTML 已经被 RSSParser 删除。
        # ------------------------------------------------------
        link_source = (
            raw_summary
            or summary
        )

        links = self._extract_links(
            link_source
        )

        print(
            f"[票务检测] {feed.name}: "
            f"发现 {len(links)} 个原始链接"
        )

        if not links:
            print(
                f"[票务检测] {feed.name}: "
                "RSS 原始正文没有可跟随的 HTTP 链接"
            )

            return summary

        evidence: List[str] = []

        # 最多尝试 8 个 URL，
        # 最多保存 2 个真正有效的项目页。
        for link in links[:8]:
            print(
                "[票务检测] "
                f"尝试外链 -> {link}"
            )

            item = (
                self._fetch_ticket_page(
                    link
                )
            )

            if not item:
                continue

            evidence.append(
                item
            )

            if len(evidence) >= 2:
                break

        if not evidence:
            print(
                f"[票务检测] {feed.name}: "
                "发现链接，但没有获得可读取的真实票务项目页"
            )

            return summary

        appendix = "\n\n".join(
            (
                "[真实票务项目页证据 "
                f"{index}] "
                f"{item}"
            )
            for index, item
            in enumerate(
                evidence,
                start=1,
            )
        )

        print(
            f"[票务检测] {feed.name}: "
            f"成功补充 "
            f"{len(evidence)} "
            "个真实项目页"
        )

        return (
            f"{summary}\n\n"
            "========== "
            "票务项目页补充证据 "
            "==========\n"
            f"{appendix}"
        )
    def fetch_feed(
        self,
        feed: RSSFeedConfig,
    ) -> Tuple[
        List[RSSItem],
        Optional[str],
    ]:
        """抓取单个 RSS 源"""

        try:
            request_url = feed.url

            response = (
                self.session.get(
                    request_url,
                    timeout=(
                        10,
                        self.timeout,
                    ),
                    allow_redirects=True,
                )
            )

            # 如果有人仍然误填 rsshub.app，
            # 自动切换到已经验证可用的 worker。
            if (
                response.status_code
                == 403
                and request_url.startswith(
                    self.RSSHUB_FALLBACK_FROM
                )
            ):
                fallback_url = (
                    request_url.replace(
                        self.RSSHUB_FALLBACK_FROM,
                        self.RSSHUB_FALLBACK_TO,
                        1,
                    )
                )

                print(
                    f"[RSS] {feed.name}: "
                    "rsshub.app 返回403，"
                    "尝试备用实例: "
                    f"{fallback_url}"
                )

                response = (
                    self.session.get(
                        fallback_url,
                        timeout=(
                            10,
                            self.timeout,
                        ),
                        allow_redirects=True,
                    )
                )

            response.raise_for_status()

            content_type = (
                response.headers.get(
                    "Content-Type",
                    "",
                ).lower()
            )

            content = response.text

            stripped = (
                content.lstrip().lower()
            )

            looks_like_feed = (
                stripped.startswith(
                    "<?xml"
                )
                or stripped.startswith(
                    "<rss"
                )
                or stripped.startswith(
                    "<feed"
                )
                or stripped.startswith(
                    "{"
                )
            )

            if not looks_like_feed:
                raise ValueError(
                    "返回内容不是 "
                    "RSS/Atom/JSON Feed；"
                    f"status="
                    f"{response.status_code}, "
                    f"content_type="
                    f"{content_type}, "
                    f"final_url="
                    f"{response.url}, "
                    f"preview="
                    f"{content[:200]!r}"
                )

            parsed_items = (
                self.parser.parse(
                    content,
                    feed.url,
                )
            )

            if feed.max_items > 0:
                parsed_items = (
                    parsed_items[
                        :feed.max_items
                    ]
                )

            now = get_configured_time(
                self.timezone
            )

            crawl_time = (
                now.strftime(
                    "%H:%M"
                )
            )

            items: List[RSSItem] = []

            author_filtered = 0

            for parsed in parsed_items:

                # 对山东/山西/宁波
                # 只允许官方作者
                if not self._author_allowed(
                    feed,
                    parsed.author or "",
                ):
                    author_filtered += 1
                    continue

                enriched_summary = (
                    self._enrich_ticket_summary(
                        feed=feed,
                        title=(
                            parsed.title
                            or ""
                        ),
                        summary=(
                            parsed.summary
                            or ""
                        ),
                    )
                )

                item = RSSItem(
                    title=parsed.title,
                    feed_id=feed.id,
                    feed_name=feed.name,
                    url=parsed.url,
                    guid=(
                        parsed.guid
                        or ""
                    ),
                    published_at=(
                        parsed.published_at
                        or ""
                    ),
                    summary=(
                        enriched_summary
                    ),
                    author=(
                        parsed.author
                        or ""
                    ),
                    crawl_time=crawl_time,
                    first_time=crawl_time,
                    last_time=crawl_time,
                    count=1,
                )

                items.append(item)

            extra = ""

            if feed.allowed_authors:
                extra = (
                    ", 官方作者过滤后"
                    f"保留 {len(items)} 条"
                    f", 丢弃 "
                    f"{author_filtered} 条"
                )

            print(
                f"[RSS] {feed.name}: "
                f"获取 {len(items)} 条 "
                f"(status="
                f"{response.status_code}, "
                f"content-type="
                f"{content_type}"
                f"{extra})"
            )

            return (
                items,
                None,
            )

        except requests.Timeout:
            error = (
                f"请求超时 "
                f"({self.timeout}s)"
            )

            print(
                f"[RSS] "
                f"{feed.name}: "
                f"{error}"
            )

            return (
                [],
                error,
            )

        except requests.RequestException as exc:
            error = (
                f"请求失败: {exc}"
            )

            print(
                f"[RSS] "
                f"{feed.name}: "
                f"{error}"
            )

            return (
                [],
                error,
            )

        except ValueError as exc:
            error = (
                f"解析失败: {exc}"
            )

            print(
                f"[RSS] "
                f"{feed.name}: "
                f"{error}"
            )

            return (
                [],
                error,
            )

        except Exception as exc:
            error = (
                "未知错误: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                f"[RSS] "
                f"{feed.name}: "
                f"{error}"
            )

            return (
                [],
                error,
            )

    def fetch_all(
        self,
    ) -> RSSData:
        """抓取所有 RSS 源"""

        all_items: Dict[
            str,
            List[RSSItem],
        ] = {}

        id_to_name: Dict[
            str,
            str,
        ] = {}

        failed_ids: List[str] = []

        now = get_configured_time(
            self.timezone
        )

        crawl_time = now.strftime(
            "%H:%M"
        )

        crawl_date = now.strftime(
            "%Y-%m-%d"
        )

        print(
            "[RSS] 开始抓取 "
            f"{len(self.feeds)} "
            "个 RSS 源..."
        )

        for i, feed in enumerate(
            self.feeds
        ):
            if i > 0:
                interval = (
                    self.request_interval
                    / 1000
                )

                jitter = (
                    random.uniform(
                        -0.2,
                        0.2,
                    )
                    * interval
                )

                time.sleep(
                    max(
                        0,
                        interval + jitter,
                    )
                )

            items, error = (
                self.fetch_feed(
                    feed
                )
            )

            id_to_name[
                feed.id
            ] = feed.name

            if error:
                failed_ids.append(
                    feed.id
                )

            else:
                all_items[
                    feed.id
                ] = items

        total_items = sum(
            len(items)
            for items
            in all_items.values()
        )

        print(
            "[RSS] 抓取完成: "
            f"{len(all_items)} "
            "个源成功, "
            f"{len(failed_ids)} "
            "个失败, "
            f"共 {total_items} 条"
        )

        if failed_ids:
            print(
                "[RSS] 失败源 ID: "
                + ", ".join(
                    failed_ids
                )
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
        """从配置字典创建抓取器"""

        freshness_config = (
            config.get(
                "freshness_filter",
                {},
            )
        )

        freshness_enabled = (
            freshness_config.get(
                "enabled",
                True,
            )
        )

        default_max_age_days = (
            freshness_config.get(
                "max_age_days",
                3,
            )
        )

        feeds: List[
            RSSFeedConfig
        ] = []

        for feed_config in config.get(
            "feeds",
            [],
        ):
            max_age_days_raw = (
                feed_config.get(
                    "max_age_days"
                )
            )

            max_age_days = None

            if (
                max_age_days_raw
                is not None
            ):
                try:
                    max_age_days = int(
                        max_age_days_raw
                    )

                    if max_age_days < 0:
                        feed_id = (
                            feed_config.get(
                                "id",
                                "unknown",
                            )
                        )

                        print(
                            "[警告] "
                            f"RSS feed "
                            f"'{feed_id}' "
                            "的 max_age_days "
                            "为负数，将使用"
                            "全局默认值"
                        )

                        max_age_days = None

                except (
                    ValueError,
                    TypeError,
                ):
                    feed_id = (
                        feed_config.get(
                            "id",
                            "unknown",
                        )
                    )

                    print(
                        "[警告] "
                        f"RSS feed "
                        f"'{feed_id}' "
                        "的 max_age_days "
                        "格式错误："
                        f"{max_age_days_raw}"
                    )

                    max_age_days = None

            allowed_authors_raw = (
                feed_config.get(
                    "allowed_authors",
                    [],
                )
            )

            if isinstance(
                allowed_authors_raw,
                str,
            ):
                allowed_authors = [
                    allowed_authors_raw
                ]

            elif isinstance(
                allowed_authors_raw,
                list,
            ):
                allowed_authors = [
                    str(
                        author
                    ).strip()
                    for author
                    in allowed_authors_raw
                    if str(
                        author
                    ).strip()
                ]

            else:
                allowed_authors = []

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
                allowed_authors=(
                    allowed_authors
                ),
                ticket_enrich=bool(
                    feed_config.get(
                        "ticket_enrich",
                        False,
                    )
                ),
            )

            if (
                feed.id
                and feed.url
            ):
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
            freshness_enabled=(
                freshness_enabled
            ),
            default_max_age_days=(
                default_max_age_days
            ),
        )
