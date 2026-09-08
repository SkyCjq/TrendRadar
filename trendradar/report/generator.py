# coding=utf-8
"""
报告生成模块

提供报告数据准备和 HTML 生成功能：
- prepare_report_data: 准备报告数据
- generate_html_report: 生成 HTML 报告
"""

import json
import shutil
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


def prepare_report_data(
    stats: List[Dict],
    failed_ids: Optional[List] = None,
    new_titles: Optional[Dict] = None,
    id_to_name: Optional[Dict] = None,
    mode: str = "daily",
    rank_threshold: int = 3,
    show_new_section: bool = True,
) -> Dict:
    """
    准备报告数据

    Args:
        stats: 统计结果列表
        failed_ids: 失败的 ID 列表
        new_titles: 新增标题
        id_to_name: ID 到名称的映射
        mode: 报告模式 (daily/incremental/current)
        rank_threshold: 排名阈值
        show_new_section: 是否显示新增热点区域

    Returns:
        Dict: 准备好的报告数据
    """
    processed_new_titles = []

    stats_title_set = {
        t["title"]
        for stat in stats
        for t in stat.get("titles", [])
    }

    # 过滤新增标题：只保留在 stats 中存活的标题（即通过了 AI/关键词过滤的标题）
    filtered_new_titles = {}
    if new_titles and id_to_name:
        for source_id, titles_data in new_titles.items():
            filtered_titles = {}
            for title, title_data in titles_data.items():
                if title in stats_title_set:
                    filtered_titles[title] = title_data
            if filtered_titles:
                filtered_new_titles[source_id] = filtered_titles

        original_new_count = sum(len(titles) for titles in new_titles.values()) if new_titles else 0
        filtered_new_count = sum(len(titles) for titles in filtered_new_titles.values()) if filtered_new_titles else 0
        if original_new_count > 0:
            print(f"新增热点过滤后：{filtered_new_count} 条保留（原始 {original_new_count} 条）")

    # 在增量模式下或配置关闭时隐藏新增新闻区域（但计数已完成）
    # 当全部热榜条目都是新增时（首次运行），也隐藏以避免与主区域完全重复
    all_new_titles = {title for titles in filtered_new_titles.values() for title in titles}
    all_are_new = bool(all_new_titles) and all_new_titles == stats_title_set
    hide_new_section = mode == "incremental" or not show_new_section or all_are_new

    if not hide_new_section and filtered_new_titles and id_to_name:
        for source_id, titles_data in filtered_new_titles.items():
            source_name = id_to_name.get(source_id, source_id)
            source_titles = []

            for title, title_data in titles_data.items():
                url = title_data.get("url", "")
                mobile_url = title_data.get("mobileUrl", "")
                ranks = title_data.get("ranks", [])

                processed_title = {
                    "title": title,
                    "source_name": source_name,
                    "time_display": "",
                    "count": 1,
                    "ranks": ranks,
                    "rank_threshold": rank_threshold,
                    "url": url,
                    "mobile_url": mobile_url,
                    "is_new": True,
                    "rank_timeline": title_data.get("rank_timeline", []),
                }
                source_titles.append(processed_title)

            if source_titles:
                processed_new_titles.append(
                    {
                        "source_id": source_id,
                        "source_name": source_name,
                        "titles": source_titles,
                    }
                )

    processed_stats = []
    for stat in stats:
        if stat["count"] <= 0:
            continue

        processed_titles = []
        for title_data in stat["titles"]:
            processed_title = {
                "title": title_data["title"],
                "source_name": title_data["source_name"],
                "time_display": title_data["time_display"],
                "count": title_data["count"],
                "ranks": title_data["ranks"],
                "rank_threshold": title_data["rank_threshold"],
                "url": title_data.get("url", ""),
                "mobile_url": title_data.get("mobileUrl", ""),
                "is_new": title_data.get("is_new", False),
                "rank_timeline": title_data.get("rank_timeline", []),
            }
            processed_titles.append(processed_title)

        processed_stats.append(
            {
                "word": stat["word"],
                "count": stat["count"],
                "percentage": stat.get("percentage", 0),
                "titles": processed_titles,
            }
        )

    # total_new_count 始终从过滤结果计算（用于头部统计），不受 hide_new_section 影响
    total_new_count = sum(len(titles) for titles in filtered_new_titles.values())

    return {
        "stats": processed_stats,
        "new_titles": processed_new_titles,
        "failed_ids": failed_ids or [],
        "total_new_count": total_new_count,
    }


def generate_html_report(
    stats: List[Dict],
    total_titles: int,
    failed_ids: Optional[List] = None,
    new_titles: Optional[Dict] = None,
    id_to_name: Optional[Dict] = None,
    mode: str = "daily",
    update_info: Optional[Dict] = None,
    rank_threshold: int = 3,
    output_dir: str = "output",
    date_folder: str = "",
    time_filename: str = "",
    render_html_func: Optional[Callable] = None,
    report_metadata: Optional[Dict] = None,
    translate_report_func: Optional[Callable] = None,
) -> str:
    """
    生成 HTML 报告

    每次生成 HTML 后会：
    1. 保存时间戳快照到 output/html/日期/时间.html（历史记录）
    2. 复制到 output/html/latest/{mode}.html（最新报告）
    3. 复制到 output/index.html 和根目录 index.html（入口）

    Args:
        stats: 统计结果列表
        total_titles: 总标题数
        failed_ids: 失败的 ID 列表
        new_titles: 新增标题
        id_to_name: ID 到名称的映射
        mode: 报告模式 (daily/incremental/current)
        update_info: 更新信息
        rank_threshold: 排名阈值
        output_dir: 输出目录
        date_folder: 日期文件夹名称
        time_filename: 时间文件名
        render_html_func: HTML 渲染函数

    Returns:
        str: 生成的 HTML 文件路径（时间戳快照路径）
    """
    # 时间戳快照文件名
    snapshot_filename = f"{time_filename}.html"

    # 构建输出路径（扁平化结构：output/html/日期/）
    snapshot_path = Path(output_dir) / "html" / date_folder
    snapshot_path.mkdir(parents=True, exist_ok=True)
    snapshot_file = str(snapshot_path / snapshot_filename)

    # 准备报告数据
    report_data = prepare_report_data(
        stats,
        failed_ids,
        new_titles,
        id_to_name,
        mode,
        rank_threshold,
    )

    # 翻译热榜 report_data（stats/new_titles）——在 prepare_report_data 过滤之后翻译，
    # 不影响新增热点区的 title 匹配过滤，使 HTML 网页版热榜也展示译文
    if translate_report_func:
        report_data = translate_report_func(report_data)

    if report_metadata:
        _METADATA_KEYS = {
            "hotlist_total", "platform_total", "rss_matched_count",
            "rss_total_count", "rss_source_total", "rss_source_failed",
        }
        for key in _METADATA_KEYS:
            if key in report_metadata:
                report_data[key] = report_metadata[key]

    # 渲染 HTML 内容
    if render_html_func:
        html_content = render_html_func(
            report_data, total_titles, mode, update_info
        )
    else:
        # 默认简单 HTML
        html_content = f"<html><body><h1>Report</h1><pre>{report_data}</pre></body></html>"

    # 1. 保存时间戳快照（历史记录）
    with open(snapshot_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    # 2. 复制到 html/latest/{mode}.html（最新报告）
    latest_dir = Path(output_dir) / "html" / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_file = latest_dir / f"{mode}.html"
    with open(latest_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    # 3. 复制到 index.html（入口）
    # output/index.html（供 Docker Volume 挂载访问）
    output_index = Path(output_dir) / "index.html"
    with open(output_index, "w", encoding="utf-8") as f:
        f.write(html_content)

    # 根目录 index.html（供 GitHub Pages 访问）
    root_index = Path("index.html")
    with open(root_index, "w", encoding="utf-8") as f:
        f.write(html_content)

    return snapshot_file

# === v1.02: GitHub Actions Run Summary Artifact ===

RUN_SUMMARY_SCHEMA_VERSION = "1.0"


def _json_safe(value: Any) -> Any:
    """将运行期对象转换为稳定、可 JSON 序列化的数据。"""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: _json_safe(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted((_json_safe(item) for item in value), key=str)
    return str(value)


def _serialize_ai_analysis(ai_analysis: Any, enabled: bool) -> Dict[str, Any]:
    """
    序列化 AI 分析结果。

    raw_response 明确不进入 Artifact：
    - 避免与结构化 AI 字段重复；
    - 避免把 Provider 原始响应/调试内容当成稳定数据契约。
    """
    result = {
        "enabled": bool(enabled),
        "present": ai_analysis is not None,
        "success": False,
        "skipped": False,
        "error": "",
        "ai_mode": "",
        "core_trends": "",
        "sentiment_controversy": "",
        "signals": "",
        "rss_insights": "",
        "outlook_strategy": "",
        "standalone_summaries": {},
        "counts": {},
    }

    if ai_analysis is None:
        return result

    for key in (
        "success",
        "skipped",
        "error",
        "ai_mode",
        "core_trends",
        "sentiment_controversy",
        "signals",
        "rss_insights",
        "outlook_strategy",
        "standalone_summaries",
    ):
        result[key] = _json_safe(getattr(ai_analysis, key, result[key]))

    count_keys = (
        "total_news",
        "analyzed_news",
        "max_news_limit",
        "hotlist_count",
        "rss_count",
        "hotlist_analyzed",
        "rss_analyzed",
        "standalone_analyzed",
        "include_rss",
        "include_standalone",
    )
    result["counts"] = {
        key: _json_safe(getattr(ai_analysis, key, 0))
        for key in count_keys
    }
    return result


def _normalize_rss_raw_items(items: Optional[List[Dict]]) -> List[Dict]:
    """保留跨系统复用所需的 RSS 证据字段，不复制数据库对象。"""
    normalized = []
    for item in items or []:
        normalized.append(
            {
                "title": item.get("title", ""),
                "feed_id": item.get("feed_id", ""),
                "feed_name": item.get("feed_name", ""),
                "url": item.get("url", ""),
                "published_at": _json_safe(item.get("published_at", "")),
                "author": item.get("author", ""),
                "summary": item.get("summary", ""),
            }
        )
    return normalized


def _filter_rss_raw_items_for_stats(
    items: Optional[List[Dict]],
    stats: Optional[List[Dict]],
) -> List[Dict]:
    """只保留最终 RSS 统计中实际出现的原始条目，避免把未匹配候选误当成报告正文。"""
    urls = set()
    titles = set()
    for stat in stats or []:
        for item in stat.get("titles", []) or []:
            url = item.get("mobile_url") or item.get("mobileUrl") or item.get("url", "")
            title = item.get("title", "")
            if url:
                urls.add(url)
            if title:
                titles.add(title)

    if not urls and not titles:
        return []

    selected = []
    for item in items or []:
        url = item.get("url", "")
        title = item.get("title", "")
        if (url and url in urls) or (title and title in titles):
            selected.append(item)
    return selected


def _markdown_link(title: str, url: str) -> str:
    safe_title = str(title or "").replace("]", r"\]")
    if url:
        return f"[{safe_title}]({url})"
    return safe_title


def _render_stat_groups_markdown(stats: List[Dict]) -> List[str]:
    lines: List[str] = []
    if not stats:
        lines.append("_当前摘要数据中无匹配条目。_")
        return lines

    for stat in stats:
        word = stat.get("word", "未分组")
        titles = stat.get("titles", [])
        lines.append(f"### {word} ({len(titles)} 条)")
        for item in titles:
            title = item.get("title", "")
            source = item.get("source_name", "")
            url = item.get("mobile_url") or item.get("mobileUrl") or item.get("url", "")
            ranks = item.get("ranks", [])
            rank_text = f" · rank={ranks[-1]}" if ranks else ""
            source_text = f" · {source}" if source else ""
            new_text = " · NEW" if item.get("is_new") else ""
            lines.append(
                f"- {_markdown_link(title, url)}{source_text}{rank_text}{new_text}"
            )
        lines.append("")
    return lines


def _render_ai_markdown(ai: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    if not ai.get("enabled"):
        return ["_AI 分析未启用。_", ""]
    if not ai.get("present"):
        return ["_本轮没有生成 AI 分析结果。_", ""]
    if ai.get("skipped"):
        return [f"_AI 分析跳过：{ai.get('error') or '无可分析内容'}_", ""]
    if not ai.get("success"):
        return [f"_AI 分析失败：{ai.get('error') or 'unknown error'}_", ""]

    sections = (
        ("核心热点与舆情态势", "core_trends"),
        ("舆论风向与争议", "sentiment_controversy"),
        ("异动与弱信号", "signals"),
        ("RSS 深度洞察", "rss_insights"),
        ("研判与策略建议", "outlook_strategy"),
    )
    for title, key in sections:
        content = str(ai.get(key, "") or "").strip()
        if content:
            lines.extend([f"### {title}", content, ""])

    standalone = ai.get("standalone_summaries") or {}
    if standalone:
        lines.append("### 独立源摘要")
        for source_id, summary in standalone.items():
            lines.append(f"- **{source_id}**：{summary}")
        lines.append("")

    if len(lines) == 0:
        lines.extend(["_AI 分析成功，但没有可展示的结构化文本。_", ""])
    return lines


def _render_standalone_markdown(standalone: Optional[Dict]) -> List[str]:
    lines: List[str] = []
    data = standalone or {}

    for platform in data.get("platforms", []) or []:
        lines.append(f"### {platform.get('name', platform.get('id', '平台'))}")
        for item in platform.get("items", []) or []:
            url = item.get("mobileUrl") or item.get("url", "")
            lines.append(f"- {_markdown_link(item.get('title', ''), url)}")
        lines.append("")

    for feed in data.get("rss_feeds", []) or []:
        lines.append(f"### {feed.get('name', feed.get('id', 'RSS'))}")
        for item in feed.get("items", []) or []:
            published = item.get("published_at", "")
            suffix = f" · {published}" if published else ""
            lines.append(
                f"- {_markdown_link(item.get('title', ''), item.get('url', ''))}{suffix}"
            )
        lines.append("")

    if not lines:
        lines.append("_本轮没有独立展示区数据。_")
        lines.append("")
    return lines


def generate_run_summary(
    stats: List[Dict],
    failed_ids: Optional[List] = None,
    new_titles: Optional[Dict] = None,
    id_to_name: Optional[Dict] = None,
    mode: str = "daily",
    rank_threshold: int = 3,
    show_new_section: bool = True,
    ai_analysis: Any = None,
    ai_enabled: bool = False,
    rss_stats: Optional[List[Dict]] = None,
    rss_new_stats: Optional[List[Dict]] = None,
    raw_rss_items: Optional[List[Dict]] = None,
    rss_failed_ids: Optional[List[str]] = None,
    standalone_data: Optional[Dict] = None,
    report_metadata: Optional[Dict] = None,
    run_metadata: Optional[Dict] = None,
    generated_at: str = "",
    timezone: str = "",
    html_file_path: Optional[str] = None,
    output_dir: str = "output/run_summary",
) -> Dict[str, str]:
    """
    生成 GitHub Actions 可读取的 Markdown / JSON / HTML 运行快照。

    该函数只消费本轮已经产生的结构化数据，不抓取网络、不读取 B2、
    不重新执行关键词分析，也不会再次调用 AI。
    """
    summary_dir = Path(output_dir)
    summary_dir.mkdir(parents=True, exist_ok=True)

    hotlist_report = prepare_report_data(
        stats=stats or [],
        failed_ids=failed_ids,
        new_titles=new_titles,
        id_to_name=id_to_name,
        mode=mode,
        rank_threshold=rank_threshold,
        show_new_section=show_new_section,
    )
    normalized_rss_stats = _json_safe(rss_stats or [])
    normalized_rss_new_stats = _json_safe(rss_new_stats or [])
    matched_raw_rss_items = _filter_rss_raw_items_for_stats(raw_rss_items, rss_stats)
    normalized_raw_rss = _normalize_rss_raw_items(matched_raw_rss_items)
    normalized_standalone = _json_safe(
        standalone_data or {"platforms": [], "rss_feeds": []}
    )
    ai_payload = _serialize_ai_analysis(ai_analysis, ai_enabled)

    metadata = report_metadata or {}
    hotlist_failed_ids = list(failed_ids or [])
    rss_failed_ids = list(rss_failed_ids or [])

    # AI 已配置但本轮缺失 / 跳过 / 失败时，正文覆盖不完整，标记为 PARTIAL。
    ai_incomplete = (
        ai_payload.get("enabled")
        and not ai_payload.get("success")
    )
    overall = (
        "PARTIAL"
        if hotlist_failed_ids or rss_failed_ids or ai_incomplete
        else "OK"
    )

    payload = {
        "schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at,
        "timezone": timezone,
        "report_mode": mode,
        "run": _json_safe(run_metadata or {}),
        "health": {
            "overall": overall,
            "hotlist": {
                "platform_total": metadata.get("platform_total", 0),
                "total_count": metadata.get("hotlist_total", 0),
                "failed_platform_ids": hotlist_failed_ids,
            },
            "rss": {
                "source_total": metadata.get("rss_source_total", 0),
                "source_failed": metadata.get("rss_source_failed", len(rss_failed_ids)),
                "failed_source_ids": rss_failed_ids,
                "matched_count": metadata.get("rss_matched_count", 0),
                "total_count": metadata.get("rss_total_count", len(normalized_raw_rss)),
            },
            "ai": {
                "enabled": ai_payload.get("enabled", False),
                "present": ai_payload.get("present", False),
                "success": ai_payload.get("success", False),
                "skipped": ai_payload.get("skipped", False),
                "error": ai_payload.get("error", ""),
            },
        },
        "ai_analysis": ai_payload,
        "hotlist": {
            "stats": hotlist_report["stats"],
            "new_titles": hotlist_report["new_titles"],
            "total_new_count": hotlist_report["total_new_count"],
        },
        "rss": {
            "stats": normalized_rss_stats,
            "new_stats": normalized_rss_new_stats,
            "items": normalized_raw_rss,
        },
        "standalone": normalized_standalone,
    }

    json_path = summary_dir / "run_summary.json"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    run = payload["run"]
    md: List[str] = [
        "# TrendRadar Run Summary",
        "",
        "## Run",
        f"- Run ID: `{run.get('id', '')}`",
        f"- Run Number: `{run.get('number', '')}`",
        f"- Run Attempt: `{run.get('attempt', '')}`",
        f"- Event: `{run.get('event', '')}`",
        f"- SHA: `{run.get('sha', '')}`",
        f"- Ref: `{run.get('ref', '')}`",
        f"- Report Mode: `{mode}`",
        f"- Generated At: `{generated_at}`",
        f"- Timezone: `{timezone}`",
        "",
        "## Data Health",
        f"- Overall: **{overall}**",
        (
            "- Hotlist: "
            f"{metadata.get('platform_total', 0)} platforms; "
            f"failed={len(hotlist_failed_ids)}"
        ),
        (
            "- RSS: "
            f"{metadata.get('rss_source_total', 0)} sources; "
            f"failed={metadata.get('rss_source_failed', len(rss_failed_ids))}; "
            f"matched={metadata.get('rss_matched_count', 0)}"
        ),
        (
            "- AI: "
            + (
                "success"
                if ai_payload.get("success")
                else "skipped"
                if ai_payload.get("skipped")
                else "failed"
                if ai_incomplete and ai_payload.get("present")
                else "not-run"
            )
        ),
        "",
        "## AI Analysis",
        "",
    ]
    md.extend(_render_ai_markdown(ai_payload))

    md.extend(["## Hotlist", ""])
    md.extend(_render_stat_groups_markdown(hotlist_report["stats"]))

    md.extend(["## RSS", ""])
    md.extend(_render_stat_groups_markdown(rss_stats or []))

    md.extend(["## Official / Standalone", ""])
    md.extend(_render_standalone_markdown(standalone_data))

    md.extend(["## Coverage Gaps", ""])
    if not hotlist_failed_ids and not rss_failed_ids:
        md.append("_本轮没有记录到数据源抓取失败。_")
    else:
        if hotlist_failed_ids:
            md.append(
                "- Hotlist failed: "
                + ", ".join(f"`{item}`" for item in hotlist_failed_ids)
            )
        if rss_failed_ids:
            md.append(
                "- RSS failed / circuit-skipped: "
                + ", ".join(f"`{item}`" for item in rss_failed_ids)
            )
        md.append("")
        md.append(
            "> 数据源失败表示覆盖不完整；不得据此推断对应领域“没有新闻”。"
        )
    md.append("")

    md_path = summary_dir / "run_summary.md"
    md_path.write_text("\n".join(md).rstrip() + "\n", encoding="utf-8")

    # 复用本轮已经生成的 HTML，不重新渲染。
    copied_index = ""
    root_index = Path("index.html")
    if root_index.is_file():
        target = summary_dir / "index.html"
        shutil.copy2(root_index, target)
        copied_index = str(target)

    copied_latest = ""
    latest_source = Path("output") / "html" / "latest" / f"{mode}.html"
    if not latest_source.is_file() and html_file_path:
        fallback_html = Path(html_file_path)
        if fallback_html.is_file():
            latest_source = fallback_html

    if latest_source.is_file():
        target = summary_dir / "latest.html"
        shutil.copy2(latest_source, target)
        copied_latest = str(target)

    return {
        "json": str(json_path),
        "markdown": str(md_path),
        "index_html": copied_index,
        "latest_html": copied_latest,
    }

