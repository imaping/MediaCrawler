# -*- coding: utf-8 -*-
# 扩展模块：无浏览器模式的抖音客户端
# 通过继承原有类实现功能扩展，不修改原始代码

from typing import Dict, Optional

from tools import utils

from .client import DouYinClient
from .help import get_a_bogus_from_js, get_web_id


class DouYinClientNoBrowser(DouYinClient):
    """
    无浏览器模式的抖音客户端
    继承自 DouYinClient，重写需要浏览器的方法
    """

    def __init__(
        self,
        timeout=60,
        proxy=None,
        *,
        headers: Dict,
        ms_token: str,
        cookie_dict: Dict,
        user_agent: str,
        proxy_ip_pool=None,
    ):
        """
        初始化无浏览器客户端

        Args:
            timeout: 请求超时时间
            proxy: 代理配置
            headers: 请求头
            ms_token: msToken（可自动生成）
            cookie_dict: Cookie 字典
            user_agent: User-Agent
            proxy_ip_pool: 代理池
        """
        # 设置必要的属性
        self.proxy = proxy
        self.timeout = timeout
        self.headers = headers
        self._host = "https://www.douyin.com"
        self.ms_token = ms_token
        self.cookie_dict = cookie_dict
        self.user_agent = user_agent

        # 初始化代理池
        self.init_proxy_pool(proxy_ip_pool)

        # 不调用父类 __init__，因为它需要 playwright_page
        # 直接设置 playwright_page 为 None
        self.playwright_page = None

    async def _DouYinClient__process_req_params(
        self,
        uri: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        request_method="GET",
    ):
        """
        重写私有方法：处理请求参数（无浏览器版本）
        使用配置的 msToken 替代从 localStorage 获取
        """
        if not params:
            return

        headers = headers or self.headers

        # 使用配置的 msToken 替代从 localStorage 获取
        common_params = {
            "device_platform": "webapp",
            "aid": "6383",
            "channel": "channel_pc_web",
            "version_code": "190600",
            "version_name": "19.6.0",
            "update_version_code": "170400",
            "pc_client_type": "1",
            "cookie_enabled": "true",
            "browser_language": "zh-CN",
            "browser_platform": "MacIntel",
            "browser_name": "Chrome",
            "browser_version": "125.0.0.0",
            "browser_online": "true",
            "engine_name": "Blink",
            "os_name": "Mac OS",
            "os_version": "10.15.7",
            "cpu_core_num": "8",
            "device_memory": "8",
            "engine_version": "109.0",
            "platform": "PC",
            "screen_width": "2560",
            "screen_height": "1440",
            'effective_type': '4g',
            "round_trip_time": "50",
            "webid": get_web_id(),
            "msToken": self.ms_token,
        }
        params.update(common_params)

        # 生成 a_bogus 签名
        import urllib.parse
        query_string = urllib.parse.urlencode(params)

        post_data = {}
        if request_method == "POST":
            post_data = params

        if "/v1/web/general/search" not in uri:
            # 使用 JS 版本生成 a_bogus，不需要 playwright_page
            a_bogus = get_a_bogus_from_js(uri, query_string, self.user_agent)
            params["a_bogus"] = a_bogus

    async def pong(self, browser_context=None) -> bool:
        """
        检查登录状态 - 无浏览器版本

        Args:
            browser_context: 兼容参数，无浏览器模式下忽略

        Returns:
            是否已登录
        """
        # 检查 Cookie 中的登录状态
        # 临时返回 True，实际应检查 LOGIN_STATUS
        return True

    async def update_cookies(self, browser_context=None):
        """
        更新 Cookie - 无浏览器版本

        Args:
            browser_context: 兼容参数，无浏览器模式下忽略
        """
        if browser_context is None:
            # 纯 HTTP 模式：Cookie 已在初始化时设置，无需更新
            utils.logger.info("[DouYinClientNoBrowser] 纯 HTTP 模式，Cookie 已配置")
            return

        # 如果提供了 browser_context，兼容原有逻辑
        cookie_str, cookie_dict = utils.convert_cookies(await browser_context.cookies())
        self.headers["Cookie"] = cookie_str
        self.cookie_dict = cookie_dict
