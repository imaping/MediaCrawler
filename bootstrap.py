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


# if __name__ == '__main__':
#     cookie_info = CookieInfo(id=None, userAgent="",
#                              cookie="passport_csrf_token=d1aa38f91ce5abfc62989325772f7f8e; passport_csrf_token_default=d1aa38f91ce5abfc62989325772f7f8e; enter_pc_once=1; UIFID_TEMP=48df845ec17e24d3e136cf9cb5f33ae5ff0d6a60613b8e70d731bae9b37d4b93159e5b004b01b2bf277354060f341bd3737ccc96276c609ece1d141d5da255ab32814e25e2ea5e896b5bb69b48566b6b; s_v_web_id=verify_mm0d3vin_D2DX8cpW_3RCX_4iEk_AbW5_9vtu82owa6gd; hevc_supported=true; fpk1=U2FsdGVkX1+4Eb/gkeXp5MbXPO7bsAwdLQVYcWjyd3WJEBtii4iMsa2tods2jzVYY8QjQ39GrAJlhwxHoqMRsg==; fpk2=1077871f5a238c0cd9e1f788c8174192; bd_ticket_guard_client_web_domain=2; UIFID=48df845ec17e24d3e136cf9cb5f33ae5ff0d6a60613b8e70d731bae9b37d4b93478065b9f3888deee63d36e530346b954a1318dce11717d0be23c3c8d90785c7ecc63e3d0e22dcea1ebdf2cd26fe2674a5f74b65f3f75461e0fd08c3c910292ecf802b323991c2cf73bd2f0c03275cdff6715c56df0fc60f326975f7bc0b74274c5ea40b200f0de6d982ec86f316fa407c346584d5322a446710648249c98ea0; xgplayer_device_id=9396751937; xgplayer_user_id=14127885942; SEARCH_RESULT_LIST_TYPE=%22single%22; passport_mfa_token=CjVrpzb%2B2U34VE6yL%2BpeAetgANjf29kVJaI7jmDPNTz%2Bpe9nIz4UylBDzxTPJaCqaF%2FP012lPRpKCjwAAAAAAAAAAAAAUB3AYXd0Ytp4C84uhbNzuHJOqN%2FExi0w6%2BK9eXQAcz7bgEZd7cW5UVwJI0LFGmHRs64Q4MaKDhj2sdFsIAIiAQOSGD9Z; d_ticket=f69069c341b1107a8e526cec7357161e44b1e; n_mh=ce1J7u7_Xrfas3m1JibC8co5Q-rNAiP56rqapzxYAEI; __security_server_data_status=1; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.54%7D; SEARCH_UN_LOGIN_PV_CURR_DAY=%7B%22date%22%3A1774020841137%2C%22count%22%3A1%7D; passport_assist_user=Cjz4D2FyiP1pv_wLju9UmM36S2KRldKG-Dn4UGuyaVEhRyBTHfNLKsGZBmXeClV3gRFpPBvZYRZDdm0cvx0aSgo8AAAAAAAAAAAAAFA2zJARuUzJv43RMwCo31Glg3QnMvQSMAaQJoM7EsepXAPnkNEluWXlPc3_hp2RltruENXqjA4Yia_WVCABIgED6haOJA%3D%3D; sid_guard=19aa3ca1a33f3041239f144cdd15dda4%7C1774231989%7C5184000%7CFri%2C+22-May-2026+02%3A13%3A09+GMT; uid_tt=cb0ebd882698044dc5a99e88bc86bb0d; uid_tt_ss=cb0ebd882698044dc5a99e88bc86bb0d; sid_tt=19aa3ca1a33f3041239f144cdd15dda4; sessionid=19aa3ca1a33f3041239f144cdd15dda4; sessionid_ss=19aa3ca1a33f3041239f144cdd15dda4; session_tlb_tag=sttt%7C6%7CGao8oaM_MEEjnxRM3RXdpP_________TOZ9g6Sx6R0gmc_LAFsf8hbuBk22S6Igxo8aUh6AD49Q%3D; is_staff_user=false; sid_ucp_v1=1.0.0-KDAzYmZlM2MxNWQ0YmRhNjJiZmYzY2JhOTM4MTcxYTgyM2IwNzZkM2YKHwju6-D82AIQtcOCzgYY7zEgDDDgxfbTBTgHQPQHSAQaAmxmIiAxOWFhM2NhMWEzM2YzMDQxMjM5ZjE0NGNkZDE1ZGRhNA; ssid_ucp_v1=1.0.0-KDAzYmZlM2MxNWQ0YmRhNjJiZmYzY2JhOTM4MTcxYTgyM2IwNzZkM2YKHwju6-D82AIQtcOCzgYY7zEgDDDgxfbTBTgHQPQHSAQaAmxmIiAxOWFhM2NhMWEzM2YzMDQxMjM5ZjE0NGNkZDE1ZGRhNA; _bd_ticket_crypt_cookie=67e4c3de24f1e1ac19ec6ea20ff0b890; __security_mc_1_s_sdk_sign_data_key_web_protect=3c6fa213-42cb-a670; __security_mc_1_s_sdk_cert_key=785dbeeb-4fb8-8c26; __security_mc_1_s_sdk_crypt_sdk=3b6f4785-4f98-8b2b; login_time=1774231989489; __ac_nonce=069d47a0a0099dda84662; __ac_signature=_02B4Z6wo00f01Yi02UAAAIDBBvmvdWl8TY2IlN3AAAv6d1; dy_swidth=2560; dy_sheight=1440; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A2560%2C%5C%22screen_height%5C%22%3A1440%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A24%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A150%7D%22; strategyABtestKey=%221775532557.595%22; ttwid=1%7CZ4sWwAnpdzjVi9FyVz8OtbLR2dUDuPz53z5-cuVTrMA%7C1775532557%7Ca3b3ae8d32195eeb8a5e7567ec9f8e3163e3ad58afbead6f500e4582bd6d73c2; SelfTabRedDotControl=%5B%7B%22id%22%3A%226960259039129257997%22%2C%22u%22%3A247%2C%22c%22%3A0%7D%2C%7B%22id%22%3A%227584346661699192841%22%2C%22u%22%3A29%2C%22c%22%3A0%7D%2C%7B%22id%22%3A%227589450888939309066%22%2C%22u%22%3A114%2C%22c%22%3A0%7D%2C%7B%22id%22%3A%227595487733360658486%22%2C%22u%22%3A90%2C%22c%22%3A0%7D%2C%7B%22id%22%3A%227267730517846411327%22%2C%22u%22%3A16%2C%22c%22%3A0%7D%2C%7B%22id%22%3A%227143503392185419789%22%2C%22u%22%3A65%2C%22c%22%3A0%7D%5D; FOLLOW_LIVE_POINT_INFO=%22MS4wLjABAAAAOeEwDPu_MIpQCL-opQ8OEfgRLdnt3M2JLemAHSSdGmo%2F1775577600000%2F0%2F1775532557915%2F0%22; is_dash_user=1; =douyin.com; device_web_cpu_core=24; device_web_memory_size=8; architecture=amd64; publish_badge_show_info=%220%2C0%2C0%2C1775532572369%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCQWNBL1BXWStVRjNENmdPSHVMZnJXd0FnK2VrQm1CQ3R3SC9LL3gzd3A5azR6OW1qYWtBaGhacFZlaVVjeDFIb3MzOSttU3pHbEZWWjJiUXZBUkFyeVE9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoyfQ%3D%3D; home_can_add_dy_2_desktop=%221%22; odin_tt=2ccc2f8d0bcd4e38802580ff94f6bc7ec3f7cf28a879fc3548d565a271cbac2b171bc3b1f8aeb1efffc5d574a63d43f8d317415670aa5d9e4c1dc07fb1176d8b60a9383ee349ef3300b694a2edae6fe7; biz_trace_id=f66fcc07; bd_ticket_guard_client_data_v2=eyJyZWVfcHVibGljX2tleSI6IkJBY0EvUFdZK1VGM0Q2Z09IdUxmcld3QWcrZWtCbUJDdHdIL0sveDN3cDlrNHo5bWpha0FoaFpwVmVpVWN4MUhvczM5K21TekdsRlZaMmJRdkFSQXJ5UT0iLCJ0c19zaWduIjoidHMuMi42YmUxN2MzMjljMTg2NDNkNTM2ODYxZWQxMmRlNzdjZTQ0ZDUyNzc1MTQ3ZGM3OTA0NmRlMWNkZWI5ODc5MmQ0YzRmYmU4N2QyMzE5Y2YwNTMxODYyNGNlZGExNDkxMWNhNDA2ZGVkYmViZWRkYjJlMzBmY2U4ZDRmYTAyNTc1ZCIsInJlcV9jb250ZW50Ijoic2VjX3RzIiwicmVxX3NpZ24iOiJQUzN1VGpaclhpbWN1c2Y3VU9QdWRtUzdwVllSVjNBc0hqRzdST3JkMjNRPSIsInNlY190cyI6IiNpSGJqUmUrZzZqZDhyT3puejQvOWxOWFQ1K1VoUjlWWklJVGFTQTBtYWoxV1V2U2ZYcklnVXJROHcrUWIifQ%3D%3D; IsDouyinActive=true;")
#     asyncio.run(get_metadata("7580554576084932603", cookie_info, node_path=""))
