import asyncio
import json
import os
from dataclasses import dataclass
from typing import Optional

import temporalio.exceptions
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.exceptions import ApplicationError

# 导入 MediaCrawler 相关模块
import config
from main import CrawlerFactory
from media_platform.douyin.exception import DataFetchError
from tools.utils import utils


@dataclass
class VideoMetadata:
    """视频元数据"""
    aweme_id: str  # 视频ID
    title: str  # 视频标题
    desc: str  # 视频描述
    nickname: str  # 作者昵称
    user_id: str  # 作者用户ID
    aweme_url: str  # 视频页面URL
    aweme_type: Optional[str] = None  # 视频类型
    create_time: Optional[int] = None  # 创建时间戳
    sec_uid: Optional[str] = None  # 作者安全ID
    short_user_id: Optional[str] = None  # 作者短ID
    user_unique_id: Optional[str] = None  # 作者唯一ID
    user_signature: Optional[str] = None  # 作者签名
    avatar: Optional[str] = None  # 作者头像URL
    liked_count: Optional[str] = None  # 点赞数（字符串类型）
    collected_count: Optional[str] = None  # 收藏数（字符串类型）
    comment_count: Optional[str] = None  # 评论数（字符串类型）
    share_count: Optional[str] = None  # 分享数（字符串类型）
    ip_location: Optional[str] = None  # IP归属地
    last_modify_ts: Optional[int] = None  # 最后修改时间戳
    cover_url: Optional[str] = None  # 封面图URL
    video_download_url: Optional[str] = None  # 视频下载URL
    music_download_url: Optional[str] = None  # 音乐下载URL
    note_download_url: Optional[str] = None  # 图文下载URL
    source_keyword: Optional[str] = None  # 来源关键词
    raw_metadata: Optional[dict] = None  # 原始完整元数据


@dataclass
class CookieInfo:
    """视频元数据"""
    id: int  # 视频ID
    cookie: str  # 视频标题
    userAgent: str  # 视频描述


async def _get_metadata_(video_id: str):
    """
    读取 detail_contents JSON 文件，提取指定视频的元数据

    Args:
        video_id: 抖音视频ID
    """
    # 获取当前日期，构建 JSON 文件路径
    current_date = utils.get_current_date()
    json_file_path = f"data/douyin/json/detail_contents_{current_date}.json"

    # 检查 JSON 文件是否存在
    if not os.path.exists(json_file_path):
        utils.logger.warning(f"[_save_metadata_to_video_dir] JSON 文件不存在: {json_file_path}")
        return

    # 读取 JSON 文件
    with open(json_file_path, 'r', encoding='utf-8') as f:
        contents_data = json.load(f)

    # 查找匹配的视频元数据（可能有重复记录，取最后一条）
    video_metadata = None
    for item in contents_data:
        if item.get("aweme_id") == video_id:
            video_metadata = item

    if not video_metadata:
        utils.logger.warning(f"[_save_metadata_to_video_dir] 未找到视频 {video_id} 的元数据")
        return

    return video_metadata


@activity.defn(name="getVideoMetadata")
async def get_metadata(video_id: str, cookie: CookieInfo) -> VideoMetadata:
    """
    获取抖音视频元数据

    Args:
        cookie: cookie 信息
        video_id: 抖音视频ID，例如 "7589312341498416434"

    Returns:
        VideoMetadata: 视频元数据对象，包含视频信息和本地文件路径
    """
    activity.logger.info(f"开始获取抖音视频元数据: {video_id}")

    try:
        # 配置爬虫参数（模拟命令行参数 --platform dy --lt cookie --type detail --specified_id xxx）
        config.PLATFORM = "dy"  # 抖音平台
        # config.LOGIN_TYPE = "cookie"  # Cookie登录方式
        config.CRAWLER_TYPE = "detail"  # 详情模式
        config.ENABLE_GET_COMMENTS = False
        config.ENABLE_GET_MEIDAS = False
        config.ENABLE_NO_BROWSER_MODE = True
        config.COOKIES = cookie.cookie
        config.DY_SPECIFIED_ID_LIST = [video_id]  # 指定视频ID列表

        activity.logger.info(f"配置参数: platform={config.PLATFORM}, login_type={config.LOGIN_TYPE}, type={config.CRAWLER_TYPE}")

        # 创建抖音爬虫实例
        crawler = CrawlerFactory.create_crawler(platform=config.PLATFORM)

        # 启动爬虫开始下载
        await crawler.start()

        # 下载完成后，获取视频元数据
        activity.logger.info(f"视频下载完成，开始获取元数据: {video_id}")
        raw_metadata = await _get_metadata_(video_id)
        if not raw_metadata:
            raise ApplicationError(
                str(f'video_id={video_id}不正确，无法获取元数据'),
                non_retryable=False,
                type='ID_NOT_FOUND'
            )

        # 提取关键字段构建 VideoMetadata 对象（直接映射 JSON 字段）
        metadata = VideoMetadata(
            aweme_id=raw_metadata.get("aweme_id", video_id),
            title=raw_metadata.get("title", ""),
            desc=raw_metadata.get("desc", ""),
            nickname=raw_metadata.get("nickname", ""),
            user_id=raw_metadata.get("user_id", ""),
            aweme_url=raw_metadata.get("aweme_url", ""),
            aweme_type=raw_metadata.get("aweme_type"),
            create_time=raw_metadata.get("create_time"),
            sec_uid=raw_metadata.get("sec_uid"),
            short_user_id=raw_metadata.get("short_user_id"),
            user_unique_id=raw_metadata.get("user_unique_id"),
            user_signature=raw_metadata.get("user_signature"),
            avatar=raw_metadata.get("avatar"),
            liked_count=raw_metadata.get("liked_count"),
            collected_count=raw_metadata.get("collected_count"),
            comment_count=raw_metadata.get("comment_count"),
            share_count=raw_metadata.get("share_count"),
            ip_location=raw_metadata.get("ip_location"),
            last_modify_ts=raw_metadata.get("last_modify_ts"),
            cover_url=raw_metadata.get("cover_url"),
            video_download_url=raw_metadata.get("video_download_url"),
            music_download_url=raw_metadata.get("music_download_url"),
            note_download_url=raw_metadata.get("note_download_url"),
            source_keyword=raw_metadata.get("source_keyword"),
            raw_metadata=raw_metadata
        )

        activity.logger.info(f"视频元数据获取成功: {video_id}, 标题: {metadata.title}")
        return metadata
    except DataFetchError as ex:
        utils.logger.error(f"[DouYinCrawler.get_aweme_detail] Get aweme detail error: {ex}")
        raise ApplicationError(
            str(ex),
            non_retryable=False,
            type='DATA_FETCH_ERROR'
        )

    except KeyError as ex:
        utils.logger.error(f"[DouYinCrawler.get_aweme_detail] have not fund note detail aweme_id:{video_id}, err: {ex}")
        raise ApplicationError(
            str(ex),
            non_retryable=False,
            type='ID_NOT_FOUND'
        )
    except ApplicationError as ex:
        raise ex
    except Exception as ex:
        utils.logger.error(f"获取视频元数据失败: {video_id}, 错误: {str(ex)}")
        raise ApplicationError(
            str(ex),
            non_retryable=False,
            type='OTHER'
        )


# 启动 Worker
async def main():
    # 连接到 Temporal Server
    client = await Client.connect("192.168.1.4:7233")

    # 创建 Worker，监听名为 "ivideo-task-queue" 的队列
    worker = Worker(
        client,
        task_queue="ivideo-py-task-queue",
        activities=[get_metadata],
    )

    print("Python Video Worker 已启动，等待任务...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
