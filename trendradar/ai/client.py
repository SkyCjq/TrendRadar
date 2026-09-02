# coding=utf-8
"""
AI 客户端模块

基于 LiteLLM 的统一 AI 模型接口。

扩展：
1. 主 API Key：AI_API_KEY
2. 备用 API Key：AI_API_KEY_FALLBACK
3. 主 Key 遇到限流、认证、超时或服务异常时，
   自动使用备用 Key 重试同一个模型。
"""

import os
from typing import Any, Dict, List

import litellm
from litellm import completion


class AIClient:
    """统一的 AI 客户端（基于 LiteLLM）"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化 AI 客户端

        Args:
            config: AI 配置字典
                - MODEL
                - API_KEY
                - API_BASE
                - TEMPERATURE
                - MAX_TOKENS
                - TIMEOUT
                - NUM_RETRIES
                - FALLBACK_MODELS

        环境变量：
            AI_API_KEY
            AI_API_KEY_FALLBACK
        """

        self.model = config.get(
            "MODEL",
            "deepseek/deepseek-chat",
        )

        # 主 Key
        self.api_key = (
            config.get("API_KEY")
            or os.environ.get("AI_API_KEY", "")
        )

        # 备用 Key
        self.fallback_api_key = os.environ.get(
            "AI_API_KEY_FALLBACK",
            "",
        )

        self.api_base = config.get(
            "API_BASE",
            "",
        )

        self.temperature = config.get(
            "TEMPERATURE",
            1.0,
        )

        self.max_tokens = config.get(
            "MAX_TOKENS",
            5000,
        )

        self.timeout = config.get(
            "TIMEOUT",
            120,
        )

        self.num_retries = config.get(
            "NUM_RETRIES",
            2,
        )

        self.fallback_models = config.get(
            "FALLBACK_MODELS",
            [],
        )

    @staticmethod
    def _should_switch_api_key(exc: Exception) -> bool:
        """
        判断当前异常是否适合切换备用 API Key。

        典型场景：
        - 401：Key 失效
        - 403：权限/项目限制
        - 408：请求超时
        - 429：RPM/RPD/TPM 配额耗尽
        - 5xx：Provider 临时服务异常

        400 等请求参数错误不切 Key，
        因为更换 Key 通常不能解决参数问题。
        """

        if isinstance(
            exc,
            (
                litellm.RateLimitError,
                litellm.AuthenticationError,
                litellm.APIConnectionError,
                litellm.Timeout,
            ),
        ):
            return True

        status_code = getattr(
            exc,
            "status_code",
            None,
        )

        return status_code in {
            401,
            403,
            408,
            429,
            500,
            502,
            503,
            504,
        }

    def _build_params(
        self,
        messages: List[Dict[str, str]],
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """构建 LiteLLM 调用参数。"""

        params = {
            "model": self.model,
            "messages": messages,
            "temperature": kwargs.get(
                "temperature",
                self.temperature,
            ),
            "timeout": kwargs.get(
                "timeout",
                self.timeout,
            ),
            "num_retries": kwargs.get(
                "num_retries",
                self.num_retries,
            ),
        }

        if self.api_base:
            params["api_base"] = self.api_base

        max_tokens = kwargs.get(
            "max_tokens",
            self.max_tokens,
        )

        if max_tokens and max_tokens > 0:
            params["max_tokens"] = max_tokens

        if self.fallback_models:
            params["fallbacks"] = (
                self.fallback_models
            )

        for key, value in kwargs.items():
            if key not in params:
                params[key] = value

        return params

    @staticmethod
    def _extract_content(response) -> str:
        """统一提取 LiteLLM 返回内容。"""

        content = (
            response
            .choices[0]
            .message
            .content
        )

        if isinstance(content, list):
            content = "\n".join(
                item.get("text", str(item))
                if isinstance(item, dict)
                else str(item)
                for item in content
            )

        return content or ""

    def chat(
        self,
        messages: List[Dict[str, str]],
        **kwargs,
    ) -> str:
        """
        调用 AI。

        调用顺序：
        1. AI_API_KEY
        2. AI_API_KEY_FALLBACK

        只有适合 Key 切换的异常才使用备用 Key。
        """

        base_params = self._build_params(
            messages,
            kwargs,
        )

        api_keys = []

        if self.api_key:
            api_keys.append(
                ("主", self.api_key)
            )

        if (
            self.fallback_api_key
            and self.fallback_api_key
            != self.api_key
        ):
            api_keys.append(
                (
                    "备用",
                    self.fallback_api_key,
                )
            )

        if not api_keys:
            raise ValueError(
                "未配置 AI API Key"
            )

        last_exception = None

        for index, (
            key_name,
            api_key,
        ) in enumerate(api_keys):

            params = dict(base_params)
            params["api_key"] = api_key

            try:
                if index > 0:
                    print(
                        "[AI] 正在使用备用 API Key"
                    )

                response = completion(
                    **params
                )

                if index > 0:
                    print(
                        "[AI] 备用 API Key 调用成功"
                    )

                return self._extract_content(
                    response
                )

            except Exception as exc:
                last_exception = exc

                has_next_key = (
                    index
                    < len(api_keys) - 1
                )

                if (
                    has_next_key
                    and self._should_switch_api_key(
                        exc
                    )
                ):
                    print(
                        "[AI] 主 API Key 调用失败，"
                        "准备切换备用 Key："
                        f"{type(exc).__name__}"
                    )
                    continue

                raise

        if last_exception:
            raise last_exception

        raise RuntimeError(
            "AI API 调用失败"
        )

    def validate_config(
        self,
    ) -> tuple[bool, str]:
        """验证配置是否有效。"""

        if not self.model:
            return (
                False,
                "未配置 AI 模型（model）",
            )

        if (
            not self.api_key
            and not self.fallback_api_key
        ):
            return (
                False,
                (
                    "未配置 AI API Key，"
                    "请设置 AI_API_KEY "
                    "或 AI_API_KEY_FALLBACK"
                ),
            )

        if "/" not in self.model:
            return (
                False,
                (
                    f"模型格式错误: "
                    f"{self.model}，"
                    "应为 "
                    "'provider/model' 格式"
                ),
            )

        return True, ""
