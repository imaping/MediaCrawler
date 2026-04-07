# -*- mode: python ; coding: utf-8 -*-
# MediaCrawler PyInstaller Spec File
# 仅打包抖音无浏览器模式（ENABLE_NO_BROWSER_MODE = True）
# 需要: pip install playwright (不需要 playwright install)
# 运行环境需要: Node.js (用于 PyExecJS 签名)

import os
import sys
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# ==================== 数据文件 ====================
# 项目自身的数据文件
project_datas = [
    ('config', 'config'),
    ('constant', 'constant'),
    ('libs', 'libs')
]

# 第三方库的数据文件（jieba 词典）
jieba_datas = collect_data_files('jieba')

all_datas = project_datas + jieba_datas

# ==================== 隐式导入 ====================
# PyInstaller 静态分析无法发现的动态导入
hidden_imports = [
    # 抖音无浏览器模式
    'media_platform.douyin',
    'media_platform.douyin.core',
    'media_platform.douyin.client',
    'media_platform.douyin.client_no_browser',
    'media_platform.douyin.crawler_no_browser',
    'media_platform.douyin.help',
    'media_platform.douyin.field',
    'media_platform.douyin.exception',
    'media_platform.douyin.login',

    # main.py 中 CrawlerFactory 引用的所有平台（顶层 import）
    'media_platform.bilibili',
    'media_platform.bilibili.core',
    'media_platform.kuaishou',
    'media_platform.kuaishou.core',
    'media_platform.weibo',
    'media_platform.weibo.core',
    'media_platform.xhs',
    'media_platform.xhs.core',
    'media_platform.tieba',
    'media_platform.tieba.core',
    'media_platform.zhihu',
    'media_platform.zhihu.core',

    # 项目内部模块
    'cmd_arg',
    'config',
    'config.base_config',
    'config.db_config',
    'config.dy_config',
    'config.bilibili_config',
    'config.ks_config',
    'config.weibo_config',
    'config.xhs_config',
    'config.tieba_config',
    'config.zhihu_config',
    'database',
    'database.db',
    'base.base_crawler',
    'model',
    'store',
    'store.douyin',
    'store.douyin._store_impl',
    'store.excel_store_base',
    'tools',
    'tools.utils',
    'tools.crawler_util',
    'tools.app_runner',
    'tools.async_file_writer',
    'var',
    'proxy',
    'proxy.proxy_ip_pool',
    'proxy.proxy_mixin',
    'constant',

    # 第三方库
    'typer',
    'typer.core',
    'typer.main',
    'click',
    'click.core',
    'typing_extensions',
    'httpx',
    'httpx._transports',
    'httpx._transports.default',
    'pydantic',
    'pydantic.deprecated',
    'pydantic.deprecated.decorator',
    'tenacity',
    'aiofiles',
    'aiosqlite',
    'execjs',
    'execjs._external_runtime',
    'parsel',
    'pyhumps',
    'dotenv',
    'jieba',
    'wordcloud',
    'openpyxl',
    'pandas',
    'redis',
    'cryptography',

    # SQLAlchemy 方言
    'sqlalchemy',
    'sqlalchemy.dialects.sqlite',
    'sqlalchemy.dialects.mysql',
    'sqlalchemy.dialects.postgresql',

    # playwright（仅作为 import 依赖，不实际使用浏览器）
    'playwright',
    'playwright.async_api',
    'playwright._impl',
    'playwright._impl._errors',
]

# ==================== 分析 ====================
a = Analysis(
    ['bootstrap.py'],
    pathex=['.'],
    binaries=[],
    datas=all_datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['pyinstaller_runtime_hook.py'],
    excludes=[
        'tkinter',
        'unittest',
        'test',
        'tests',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ==================== 打包 ====================
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MediaCrawler',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MediaCrawler',
)
