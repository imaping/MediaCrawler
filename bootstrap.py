# Decompiled with PyLingual (https://pylingual.io)
# Internal filename: 'bootstrap.py'
# Bytecode version: 3.11a7e (3495)
# Source timestamp: 1970-01-01 00:00:00 UTC (0)

import asyncio
from dataclasses import dataclass
from typing import Optional

import config
from main import CrawlerFactory
from tools.utils import utils


@dataclass
class VideoMetadata:
    """视频元数据"""
    aweme_id: str
    title: str
    desc: str
    nickname: str
    user_id: str
    aweme_url: str
    aweme_type: Optional[str] = None
    create_time: Optional[int] = None
    sec_uid: Optional[str] = None
    short_user_id: Optional[str] = None
    user_unique_id: Optional[str] = None
    user_signature: Optional[str] = None
    avatar: Optional[str] = None
    liked_count: Optional[str] = None
    collected_count: Optional[str] = None
    comment_count: Optional[str] = None
    share_count: Optional[str] = None
    ip_location: Optional[str] = None
    last_modify_ts: Optional[int] = None
    cover_url: Optional[str] = None
    video_download_url: Optional[str] = None
    music_download_url: Optional[str] = None
    note_download_url: Optional[str] = None
    source_keyword: Optional[str] = None
    raw_metadata: Optional[dict] = None


@dataclass
class CookieInfo:
    """视频元数据"""
    id: int
    cookie: str
    userAgent: str


async def get_metadata(video_id: str, cookie: CookieInfo, node_path: str):
    """\n    获取抖音视频元数据\n\n    Args:\n        cookie: cookie 信息\n        video_id: 抖音视频ID，例如 \"7589312341498416434\"\n\n    Returns:\n        VideoMetadata: 视频元数据对象，包含视频信息和本地文件路径\n    """
    utils.logger.info(f'开始获取抖音视频元数据: {video_id}')
    try:
        config.PLATFORM = 'dy'
        config.CRAWLER_TYPE = 'detail'
        config.ENABLE_GET_COMMENTS = False
        config.ENABLE_GET_MEIDAS = False
        config.ENABLE_NO_BROWSER_MODE = True
        config.ENABLE_GET_WORDCLOUD = False
        config.COOKIES = cookie.cookie
        config.SAVE_DATA_OPTION = 'sqlite'
        config.DY_SPECIFIED_ID_LIST = [video_id]
        config.NODE_PATH = node_path
        utils.logger.info(f'配置参数: platform={config.PLATFORM}, login_type={config.LOGIN_TYPE}, type={config.CRAWLER_TYPE}')
        crawler = CrawlerFactory.create_crawler(platform=config.PLATFORM)
        await crawler.start()
        utils.logger.info(f'视频元数据获取成功: {video_id}')
    except Exception as e:
        error_info = {'status': 'error', 'message': str(e)}
        raise


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='抖音视频元数据获取工具')
    parser.add_argument('--video_id', type=str, required=True, help='抖音视频ID，例如 7589312341498416434')
    parser.add_argument('--cookie', type=str, required=True, help='抖音 Cookie 字符串')
    parser.add_argument('--user_agent', type=str, default='', help='User-Agent 字符串')
    parser.add_argument('--cookie_id', type=int, default=0, help='Cookie ID（可选）')
    parser.add_argument('--node_path', type=str, default='', help='必传')
    args = parser.parse_args()
    cookie_info = CookieInfo(id=args.cookie_id, cookie=args.cookie, userAgent=args.user_agent)
    asyncio.run(get_metadata(args.video_id, cookie_info, args.node_path))
