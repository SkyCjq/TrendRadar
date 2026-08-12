# coding=utf-8

"""
RSS 抓取器

负责从配置的 RSS 源抓取数据并转换为标准格式。

扩展能力：
1. allowed_authors
   对 RSSHub keyword 结果做官方作者精确白名单过滤。

2. ticket_enrich
   对篮球票务相关微博中的外链进行跟随。

3. 严格票务项目页识别
   微博内部页、微博访客登录页、图片 CDN、普通媒体文章
   不再被误判成“真实票务项目页”。
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


# ============================================================
# RSS Feed 配置
# ============================================================

@dataclass
class RSSFeedConfig:
    """RSS 源配置"""

    id: str
    name: str
    url: str

    max_items: int = 0
    enabled: bool = True
    max_age_days: Optional[int] = None

    # keyword RSS 官方作者白名单
    allowed_authors: List[str] = field(
        default_factory=list
    )

    # 是否对票务内容执行真实项目页增强
    ticket_enrich: bool = False


# ============================================================
# HTML 可见文字解析
# ============================================================

class _VisibleTextParser(HTMLParser):
    """从 HTML 中提取可见文字"""

    def __init__(self):
        super().__init__(
            convert_charrefs=True
        )

        self._skip_depth = 0
        self._parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in {
            "script",
            "style",
            "noscript",
            "svg",
        }:
            self._skip_depth += 1

    def handle_endtag(self, tag):
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

    def handle_data(self, data):
        if self._skip_depth != 0:
            return

        text = re.sub(
            r"\s+",
            " ",
            data,
        ).strip()

        if text:
            self._parts.append(text)

    def get_text(self) -> str:
        text = " ".join(
            self._parts
        )

        return re.sub(
            r"\s+",
            " ",
            text,
        ).strip()


# ============================================================
# RSSFetcher
# ============================================================

class RSSFetcher:
    """RSS 抓取器"""

    # --------------------------------------------------------
    # RSSHub fallback
    # --------------------------------------------------------

    RSSHUB_FALLBACK_FROM = (
        "https://rsshub.app/"
    )

    RSSHUB_FALLBACK_TO = (
        "https://rsshub.rss3.workers.dev/"
    )

    # --------------------------------------------------------
    # 票务词
    #
    # “门票”不能单独认为是售票。
    #
    # 例如：
    #   “争夺总决赛门票”
    #
    # 实际表示晋级资格。
    # --------------------------------------------------------

    TICKET_STRONG_WORDS = (
        "购票",
        "售票",
        "开票",
        "票价",
        "票务",
        "开售",
        "预售",
        "抢票",
        "余票",
        "售罄",
        "实名购票",
        "实名制",
        "退票",
        "退改",
        "限购",
        "购票链接",
        "购票渠道",
        "购票平台",
        "票星球",
        "大麦",
        "看个比赛",
        "猫眼",
    )

    TICKET_WEAK_WORDS = (
        "门票",
    )

    TICKET_TRANSACTION_WORDS = (
        "购买",
        "购票",
        "售票",
        "票价",
        "元",
        "¥",
        "￥",
        "开售",
        "预售",
        "限购",
        "实名",
    )

    # --------------------------------------------------------
    # 篮球语义词
    # --------------------------------------------------------

    BASKETBALL_WORDS = (
        "篮球",
        "cba",
        "wcba",
        "nbl",
        "男篮",
        "女篮",
        "国青",
        "u16",
        "u18",
        "u19",
        "u21",
        "篮协",
        "全明星",
        "俱乐部杯",
        "三人篮球",
        "超三",
    )

    # --------------------------------------------------------
    # 真正票务平台页面常见交易词
    #
    # 用于避免：
    # 新闻文章里仅仅提到“票星球”
    # 就被误认为票星球项目页。
    # --------------------------------------------------------

    PAGE_TICKET_WORDS = (
        "立即购票",
        "立即购买",
        "选择场次",
        "选择票档",
        "票价",
        "开售",
        "售票",
        "实名",
        "限购",
        "退票",
        "购买数量",
        "缺货登记",
        "暂无票",
        "售罄",
    )

    # --------------------------------------------------------
    # 初始化
    # --------------------------------------------------------

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
            feed
            for feed in feeds
            if feed.enabled
        ]

        self.request_interval = request_interval
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

    # ========================================================
    # HTTP Session
    # ========================================================

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
                    "application/json;q=0.8, "
                    "*/*;q=0.7"
                ),
                "Accept-Language": (
                    "zh-CN,zh;q=0.9,en;q=0.8"
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

    # ========================================================
    # Author 白名单
    # ========================================================

    @staticmethod
    def _normalize_author(
        author: str,
    ) -> str:
        """标准化作者名"""

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
        """检查 author 是否属于官方白名单"""

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

    # ========================================================
    # URL 提取
    # ========================================================

    @staticmethod
    def _extract_links(
        fragment: str,
    ) -> List[str]:
        """
        从 RSS 原始 description / summary
        中提取 HTTP 链接。
        """

        if not fragment:
            return []

        decoded = html.unescape(
            fragment
        )

        links: List[str] = []

        # ----------------------------------------------------
        # HTML href
        # ----------------------------------------------------

        for match in re.findall(
            r'href\s*=\s*["\']([^"\']+)["\']',
            decoded,
            flags=re.IGNORECASE,
        ):
            links.append(
                match.strip()
            )

        # ----------------------------------------------------
        # 纯文本 URL
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # 去重
        # ----------------------------------------------------

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

    # ========================================================
    # 票务内容判断
    # ========================================================

    def _is_ticket_context(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
    ) -> bool:
        """
        判断当前条目是否真的属于篮球票务。

        重要：
        不再使用 feed.name 判断篮球属性。

        否则：
            票务专项｜大麦篮球
        会让任何演唱会都天然带有“篮球”二字。
        """

        # 当前不用 feed.name。
        # 保留参数只是兼容现有调用接口。
        _ = feed

        text = (
            f"{title or ''} "
            f"{summary or ''}"
        )

        text_lower = text.lower()

        # ----------------------------------------------------
        # 是否篮球
        # ----------------------------------------------------

        has_basketball = any(
            word.lower() in text_lower
            for word in self.BASKETBALL_WORDS
        )

        if not has_basketball:
            return False

        # ----------------------------------------------------
        # 强票务语义
        # ----------------------------------------------------

        has_strong_ticket = any(
            word.lower() in text_lower
            for word
            in self.TICKET_STRONG_WORDS
        )

        # ----------------------------------------------------
        # 弱票务语义
        # ----------------------------------------------------

        has_weak_ticket = any(
            word.lower() in text_lower
            for word
            in self.TICKET_WEAK_WORDS
        )

        has_transaction = any(
            word.lower() in text_lower
            for word
            in self.TICKET_TRANSACTION_WORDS
        )

        has_ticket = (
            has_strong_ticket
            or (
                has_weak_ticket
                and has_transaction
            )
        )

        return has_ticket

    # ========================================================
    # URL 过滤
    # ========================================================

    @staticmethod
    def _is_static_asset_url(
        url: str,
    ) -> bool:
        """是否图片/视频/JS/CSS等静态资源"""

        path = (
            urlparse(url).path
            or ""
        ).lower()

        static_extensions = (
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".webp",
            ".svg",
            ".mp4",
            ".mov",
            ".m3u8",
            ".css",
            ".js",
            ".woff",
            ".woff2",
            ".ttf",
            ".ico",
        )

        return path.endswith(
            static_extensions
        )

    @staticmethod
    def _is_weibo_or_sina_url(
        url: str,
    ) -> bool:
        """
        排除：

        weibo.com
        m.weibo.cn
        visitor.passport.weibo.cn
        新浪图片 CDN
        微博视频页

        它们不是票务项目页面。
        """

        host = (
            urlparse(url).hostname
            or ""
        ).lower()

        blocked_hosts = (
            "weibo.com",
            "weibo.cn",
            "sinaimg.cn",
            "weibocdn.com",
            "passport.weibo.cn",
            "visitor.passport.weibo.cn",
            "h5.sinaimg.cn",
            "n.sinaimg.cn",
            "video.weibo.com",
        )

        return any(
            host == blocked
            or host.endswith(
                "." + blocked
            )
            for blocked
            in blocked_hosts
        )

    # ========================================================
    # HTML → 可见正文
    # ========================================================

    @staticmethod
    def _extract_visible_text(
        content: str,
        limit: int = 6000,
    ) -> str:
        """提取网页可见正文"""

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

    # ========================================================
    # 真正票务平台识别
    # ========================================================

    def _detect_ticket_platform(
        self,
        final_url: str,
        visible_text: str,
    ) -> Optional[str]:
        """
        判断最终页面是否确实属于票务平台。

        返回：
            大麦
            票星球
            猫眼
            看个比赛
            None
        """

        host = (
            urlparse(final_url).hostname
            or ""
        ).lower()

        text = (
            visible_text
            or ""
        )[:6000].lower()

        # ----------------------------------------------------
        # 大麦
        # 域名证据本身足够强。
        # ----------------------------------------------------

        if (
            host == "damai.cn"
            or host.endswith(
                ".damai.cn"
            )
        ):
            return "大麦"

        # ----------------------------------------------------
        # 票星球
        #
        # 如果域名中已经存在 piaoxingqiu，
        # 可直接认定。
        #
        # 否则必须：
        #   页面正文出现“票星球”
        #   +
        #   至少出现一个实际票务交易词
        # ----------------------------------------------------

        if "piaoxingqiu" in host:
            return "票星球"

        if "票星球" in text:
            if any(
                word.lower() in text
                for word
                in self.PAGE_TICKET_WORDS
            ):
                return "票星球"

        # ----------------------------------------------------
        # 猫眼
        # ----------------------------------------------------

        if (
            host == "maoyan.com"
            or host.endswith(
                ".maoyan.com"
            )
        ):
            return "猫眼"

        if "猫眼演出" in text:
            if any(
                word.lower() in text
                for word
                in self.PAGE_TICKET_WORDS
            ):
                return "猫眼"

        # ----------------------------------------------------
        # 看个比赛
        # ----------------------------------------------------

        if "kangebisai" in host:
            return "看个比赛"

        if "看个比赛" in text:
            if any(
                word.lower() in text
                for word
                in self.PAGE_TICKET_WORDS
            ):
                return "看个比赛"

        return None

    # ========================================================
    # JSON 字段解析
    # ========================================================

    @staticmethod
    def _json_string(
        content: str,
        key: str,
    ) -> Optional[str]:
        """提取 JSON 字符串字段"""

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
        """提取 JSON 数字字段"""

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
        """提取 JSON bool 字段"""

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

    # ========================================================
    # 大麦字段解析
    # ========================================================

    def _extract_damai_evidence(
        self,
        content: str,
        visible_text: str,
    ) -> str:
        """提取大麦项目详情字段"""

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

        # ----------------------------------------------------
        # 字符串字段
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # 限购
        # ----------------------------------------------------

        single_limit = (
            self._json_number(
                content,
                "singleLimit",
            )
        )

        if single_limit:
            parts.append(
                f"单场限购={single_limit}张"
            )

        # ----------------------------------------------------
        # 实名
        # ----------------------------------------------------

        real_name = (
            self._json_bool(
                content,
                "needRealNameCertified",
            )
        )

        if real_name is not None:
            if real_name:
                parts.append(
                    "实名要求=需要实名/实人认证"
                )

            else:
                parts.append(
                    "实名要求=页面结构字段显示无需实名认证"
                )

        # ----------------------------------------------------
        # 退票规则
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # 如果 JSON 没有 singleLimit，
        # 尝试正文提取
        # ----------------------------------------------------

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

    # ========================================================
    # 真实票务页面抓取
    # ========================================================

    def _fetch_ticket_page(
        self,
        source_url: str,
    ) -> Optional[str]:
        """
        跟随外链抓真实票务项目页面。

        只有真正识别到：
            大麦
            票星球
            猫眼
            看个比赛

        才返回证据。

        以下全部忽略：
            微博页
            微博 Visitor 页面
            微博搜索页
            图片 CDN
            微信普通文章
            普通新闻网页
        """

        if not source_url:
            return None

        # ----------------------------------------------------
        # 请求前过滤
        # ----------------------------------------------------

        if self._is_static_asset_url(
            source_url
        ):
            return None

        if self._is_weibo_or_sina_url(
            source_url
        ):
            return None

        # ----------------------------------------------------
        # 请求
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # 重定向后的最终地址再次检查
        # ----------------------------------------------------

        if self._is_static_asset_url(
            final_url
        ):
            return None

        if self._is_weibo_or_sina_url(
            final_url
        ):
            print(
                "[票务页] 忽略微博/新浪内部页: "
                f"{final_url}"
            )

            return None

        # ----------------------------------------------------
        # Content-Type
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # 页面正文
        # ----------------------------------------------------

        raw_content = (
            response.text[
                :2_000_000
            ]
        )

        visible_text = (
            self._extract_visible_text(
                raw_content,
                limit=6000,
            )
        )

        # ----------------------------------------------------
        # 必须是真正票务平台
        # ----------------------------------------------------

        platform = (
            self._detect_ticket_platform(
                final_url,
                visible_text,
            )
        )

        if not platform:
            print(
                "[票务页] 非票务平台页面，忽略: "
                f"{final_url}"
            )

            return None

        # ----------------------------------------------------
        # 构造证据
        # ----------------------------------------------------

        evidence_parts = [
            f"平台={platform}",
            f"最终项目链接={final_url}",
        ]

        # ----------------------------------------------------
        # 大麦结构化字段
        # ----------------------------------------------------

        if platform == "大麦":
            structured = (
                self._extract_damai_evidence(
                    raw_content,
                    visible_text,
                )
            )

            if structured:
                evidence_parts.append(
                    structured
                )

        # ----------------------------------------------------
        # 保留部分正文
        # ----------------------------------------------------

        if visible_text:
            evidence_parts.append(
                "页面正文="
                + visible_text[:3500]
            )

        print(
            "[票务页] 真正票务页面抓取成功: "
            f"{platform} -> {final_url}"
        )

        return "；".join(
            evidence_parts
        )

    # ========================================================
    # Ticket enrich
    # ========================================================

    def _enrich_ticket_summary(
        self,
        feed: RSSFeedConfig,
        title: str,
        summary: str,
        raw_summary: str = "",
    ) -> str:
        """
        对篮球票务条目进行项目页增强。

        summary:
            parser 清洗后的纯文本。

        raw_summary:
            parser 保留的原始 HTML。
            用于获取 href。
        """

        # ----------------------------------------------------
        # 当前 Feed 不需要票务增强
        # ----------------------------------------------------

        if not feed.ticket_enrich:
            return summary

        # ----------------------------------------------------
        # 不是篮球票务
        # ----------------------------------------------------

        if not self._is_ticket_context(
            feed,
            title,
            summary,
        ):
            return summary

        print(
            f"[票务检测] {feed.name}: "
            f"命中篮球票务内容 -> "
            f"{title[:80]}"
        )

        # ----------------------------------------------------
        # 必须从 raw_summary 提取链接
        # ----------------------------------------------------

        link_source = (
            raw_summary
            or summary
        )

        links = self._extract_links(
            link_source
        )

        if not links:
            print(
                f"[票务检测] {feed.name}: "
                "RSS 原始正文没有可跟随的 HTTP 链接"
            )

            return summary

        # ----------------------------------------------------
        # 去除：
        #   微博页
        #   新浪页
        #   图片
        #   视频
        #   JS/CSS
        # ----------------------------------------------------

        candidate_links: List[str] = []

        for link in links:
            if self._is_static_asset_url(
                link
            ):
                continue

            if self._is_weibo_or_sina_url(
                link
            ):
                continue

            candidate_links.append(
                link
            )

        print(
            f"[票务检测] {feed.name}: "
            f"{len(links)} 个原始链接，"
            f"{len(candidate_links)} 个可用外链候选"
        )

        # ----------------------------------------------------
        # 没有真实外链
        # ----------------------------------------------------

        if not candidate_links:
            print(
                f"[票务检测] {feed.name}: "
                "只有微博内部页/图片等链接，"
                "没有真实购票项目外链"
            )

            return summary

        evidence: List[str] = []

        # ----------------------------------------------------
        # 最多尝试 6 个外链
        # 最多保存 2 个真实项目页
        # ----------------------------------------------------

        for link in candidate_links[:6]:

            print(
                "[票务检测] "
                f"尝试票务外链 -> {link}"
            )

            item = (
                self._fetch_ticket_page(
                    link
                )
            )

            if not item:
                continue

            evidence.append(item)

            if len(evidence) >= 2:
                break

        # ----------------------------------------------------
        # 有外链，但没有真正票务项目页
        # ----------------------------------------------------

        if not evidence:
            print(
                f"[票务检测] {feed.name}: "
                "发现候选外链，"
                "但没有获得真正票务平台项目页"
            )

            return summary

        # ----------------------------------------------------
        # 拼接真实票务证据
        # ----------------------------------------------------

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
            "个真正票务项目页"
        )

        return (
            f"{summary}\n\n"
            "========== "
            "票务项目页补充证据 "
            "==========\n"
            f"{appendix}"
        )

    # ========================================================
    # 单个 RSS Feed
    # ========================================================

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

            # ------------------------------------------------
            # rsshub.app 403 fallback
            # ------------------------------------------------

            if (
                response.status_code == 403
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

            # ------------------------------------------------
            # Feed 内容
            # ------------------------------------------------

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
                    f"status={response.status_code}, "
                    f"content_type={content_type}, "
                    f"final_url={response.url}, "
                    f"preview={content[:200]!r}"
                )

            # ------------------------------------------------
            # Parser
            # ------------------------------------------------

            parsed_items = (
                self.parser.parse(
                    content,
                    feed.url,
                )
            )

            # ------------------------------------------------
            # max_items
            # ------------------------------------------------

            if feed.max_items > 0:
                parsed_items = (
                    parsed_items[
                        :feed.max_items
                    ]
                )

            # ------------------------------------------------
            # 时间
            # ------------------------------------------------

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

            # ------------------------------------------------
            # 条目处理
            # ------------------------------------------------

            for parsed in parsed_items:

                # ============================================
                # 官方作者过滤
                # ============================================

                if not self._author_allowed(
                    feed,
                    parsed.author or "",
                ):
                    author_filtered += 1

                    print(
                        f"[RSS作者过滤] {feed.name}: "
                        f"拒绝 author="
                        f"{parsed.author!r}, "
                        f"title="
                        f"{(parsed.title or '')[:60]!r}"
                    )

                    continue

                # ============================================
                # summary
                # ============================================

                clean_summary = (
                    parsed.summary
                    or ""
                )

                # parser.py 中新增的 raw_summary
                raw_summary = (
                    getattr(
                        parsed,
                        "raw_summary",
                        None,
                    )
                    or clean_summary
                )

                # ============================================
                # 真实票务项目页增强
                # ============================================

                enriched_summary = (
                    self._enrich_ticket_summary(
                        feed=feed,

                        title=(
                            parsed.title
                            or ""
                        ),

                        summary=clean_summary,

                        raw_summary=raw_summary,
                    )
                )

                # ============================================
                # RSSItem
                # ============================================

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

            # ------------------------------------------------
            # 作者过滤统计
            # ------------------------------------------------

            extra = ""

            if feed.allowed_authors:
                extra = (
                    ", 官方作者过滤后"
                    f"保留 {len(items)} 条"
                    f", 丢弃 "
                    f"{author_filtered} 条"
                )

            # ------------------------------------------------
            # 完成日志
            # ------------------------------------------------

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

        # ====================================================
        # Timeout
        # ====================================================

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

        # ====================================================
        # HTTP
        # ====================================================

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

        # ====================================================
        # Parser
        # ====================================================

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

        # ====================================================
        # Unknown
        # ====================================================

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

    # ========================================================
    # 所有 RSS Feed
    # ========================================================

    def fetch_all(
        self,
    ) -> RSSData:
        """抓取全部 RSS 源"""

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

        crawl_time = (
            now.strftime(
                "%H:%M"
            )
        )

        crawl_date = (
            now.strftime(
                "%Y-%m-%d"
            )
        )

        print(
            "[RSS] 开始抓取 "
            f"{len(self.feeds)} "
            "个 RSS 源..."
        )

        # ----------------------------------------------------
        # Feed 循环
        # ----------------------------------------------------

        for index, feed in enumerate(
            self.feeds
        ):

            # ------------------------------------------------
            # 请求间隔
            # ------------------------------------------------

            if index > 0:
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

            # ------------------------------------------------
            # 抓取
            # ------------------------------------------------

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

        # ----------------------------------------------------
        # 统计
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # 返回
        # ----------------------------------------------------

        return RSSData(
            date=crawl_date,
            crawl_time=crawl_time,
            items=all_items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
        )

    # ========================================================
    # from_config
    # ========================================================

    @classmethod
    def from_config(
        cls,
        config: Dict,
    ) -> "RSSFetcher":
        """
        从配置字典创建抓取器。

        虽然当前 TrendRadar 主程序在 __main__.py
        中也会手动创建 RSSFeedConfig，
        这里仍保留完整兼容实现。
        """

        # ----------------------------------------------------
        # freshness
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # feeds
        # ----------------------------------------------------

        for feed_config in config.get(
            "feeds",
            [],
        ):

            # =================================================
            # max_age_days
            # =================================================

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
                            "为负数，将使用全局默认值"
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

            # =================================================
            # allowed_authors
            # =================================================

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
                    allowed_authors_raw.strip()
                ]

            elif isinstance(
                allowed_authors_raw,
                list,
            ):
                allowed_authors = [
                    str(author).strip()
                    for author
                    in allowed_authors_raw
                    if str(author).strip()
                ]

            else:
                allowed_authors = []

            # =================================================
            # ticket_enrich
            # =================================================

            ticket_enrich_raw = (
                feed_config.get(
                    "ticket_enrich",
                    False,
                )
            )

            if isinstance(
                ticket_enrich_raw,
                bool,
            ):
                ticket_enrich = (
                    ticket_enrich_raw
                )

            elif isinstance(
                ticket_enrich_raw,
                str,
            ):
                ticket_enrich = (
                    ticket_enrich_raw
                    .strip()
                    .lower()
                    in {
                        "1",
                        "true",
                        "yes",
                        "on",
                    }
                )

            else:
                ticket_enrich = bool(
                    ticket_enrich_raw
                )

            # =================================================
            # RSSFeedConfig
            # =================================================

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

                max_age_days=(
                    max_age_days
                ),

                allowed_authors=(
                    allowed_authors
                ),

                ticket_enrich=(
                    ticket_enrich
                ),
            )

            if (
                feed.id
                and feed.url
            ):
                feeds.append(feed)

        # ----------------------------------------------------
        # 创建 RSSFetcher
        # ----------------------------------------------------

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
