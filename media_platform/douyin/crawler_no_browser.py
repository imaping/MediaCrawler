# -*- coding: utf-8 -*-
# 扩展模块：无浏览器模式的抖音爬虫
# 通过继承原有类实现功能扩展，不修改原始代码

import asyncio
import json
import os
import random
from typing import Optional

import config
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from tools import utils
from var import crawler_type_var

from .client_no_browser import DouYinClientNoBrowser
from .core import DouYinCrawler


def generate_ms_token(random_length=107):
    """
    生成抖音 msToken
    msToken 是一个随机字符串，由大小写字母、数字和 = 组成
    默认长度为 107 个字符
    """
    random_str = ''
    base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789='
    length = len(base_str) - 1
    for _ in range(random_length):
        random_str += base_str[random.randint(0, length)]
    return random_str


class DouYinCrawlerNoBrowser(DouYinCrawler):
    """
    无浏览器模式的抖音爬虫
    继承自 DouYinCrawler，重写需要浏览器的方法
    """

    def __init__(self) -> None:
        """初始化无浏览器爬虫"""
        self.index_url = "https://www.douyin.com"
        self.ip_proxy_pool = None
        self._load_cookies_config()

    def _load_cookies_config(self) -> None:
        """加载 Cookie 配置文件"""
        config_file = "douyin_cookies_config.json"
        if not os.path.exists(config_file):
            utils.logger.error(f"[DouYinCrawlerNoBrowser] Cookie 配置文件不存在: {config_file}")
            utils.logger.error(f"[DouYinCrawlerNoBrowser] 请复制 douyin_cookies_config.json.example 为 {config_file}")
            raise FileNotFoundError(f"请创建 {config_file} 配置文件")

        with open(config_file, 'r', encoding='utf-8') as f:
            cookie_config = json.load(f)

        self.cookie_string = cookie_config.get("cookie_string", "")
        self.ms_token = generate_ms_token()  # 自动生成 msToken
        self.user_agent = cookie_config.get(
            "user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        if not self.cookie_string:
            raise ValueError("Cookie 配置文件中缺少必需的 cookie_string")

        utils.logger.info("[DouYinCrawlerNoBrowser] Cookie 配置加载成功")
        utils.logger.info(f"[DouYinCrawlerNoBrowser] msToken 已自动生成: {self.ms_token[:20]}...")

    async def start(self) -> None:
        """启动爬虫 - 纯 HTTP 请求模式，无需浏览器"""
        httpx_proxy_format = None
        if config.ENABLE_IP_PROXY:
            self.ip_proxy_pool = await create_ip_pool(
                config.IP_PROXY_POOL_COUNT,
                enable_validate_ip=True
            )
            ip_proxy_info: IpInfoModel = await self.ip_proxy_pool.get_proxy()
            _, httpx_proxy_format = utils.format_proxy_info(ip_proxy_info)

        utils.logger.info("[DouYinCrawlerNoBrowser] 使用纯 HTTP 请求模式（无浏览器）")

        # 创建无浏览器客户端
        self.dy_client = await self._create_no_browser_client(httpx_proxy_format)

        # 检查登录状态
        if not await self.dy_client.pong():
            utils.logger.error("[DouYinCrawlerNoBrowser] Cookie 验证失败")
            raise Exception("Cookie 验证失败，请检查配置文件")

        utils.logger.info("[DouYinCrawlerNoBrowser] Cookie 验证成功，开始爬取")

        # 执行爬取任务
        crawler_type_var.set(config.CRAWLER_TYPE)
        if config.CRAWLER_TYPE == "search":
            await self.search()
        elif config.CRAWLER_TYPE == "detail":
            await self.get_specified_awemes()
        elif config.CRAWLER_TYPE == "creator":
            await self.get_creators_and_videos()

        utils.logger.info("[DouYinCrawlerNoBrowser] 爬取完成")

    async def _create_no_browser_client(self, httpx_proxy: Optional[str]):
        """创建无浏览器客户端"""
        # 从配置文件解析 Cookie
        cookie_dict = utils.convert_str_cookie_to_dict(self.cookie_string)

        client = DouYinClientNoBrowser(
            proxy=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": self.cookie_string,
                "Host": "www.douyin.com",
                "Origin": "https://www.douyin.com/",
                "Referer": "https://www.douyin.com/",
                "Content-Type": "application/json;charset=UTF-8",
            },
            ms_token=self.ms_token,
            cookie_dict=cookie_dict,
            user_agent=self.user_agent,
            proxy_ip_pool=self.ip_proxy_pool,
        )
        return client

    async def launch_browser(self, chromium, playwright_proxy, user_agent, headless=True):
        """
        实现抽象方法 - 无浏览器模式不支持
        """
        raise NotImplementedError("无浏览器模式不支持浏览器启动")

    async def close(self) -> None:
        """关闭爬虫"""
        utils.logger.info("[DouYinCrawlerNoBrowser] 爬虫已关闭")
