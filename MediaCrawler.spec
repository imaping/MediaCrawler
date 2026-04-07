# -*- mode: python ; coding: utf-8 -*-
# MediaCrawler PyInstaller Spec File
# 仅打包抖音无浏览器模式（ENABLE_NO_BROWSER_MODE = True）
# 需要: pip install playwright (不需要 playwright install)
# 运行环境需要: Node.js (用于 PyExecJS 签名)

import importlib.util
import os
import sys
from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None
project_root = Path.cwd().resolve()


def _collect_project_site_packages() -> list[str]:
    candidates: list[Path] = []
    windows_site_packages = project_root / ".venv" / "Lib" / "site-packages"
    if windows_site_packages.is_dir():
        candidates.append(windows_site_packages)

    posix_lib_root = project_root / ".venv" / "lib"
    if posix_lib_root.is_dir():
        candidates.extend(
            site_packages
            for site_packages in posix_lib_root.glob("python*/site-packages")
            if site_packages.is_dir()
        )

    return [str(path) for path in candidates]


project_site_packages = _collect_project_site_packages()
for site_packages in reversed(project_site_packages):
    if site_packages not in sys.path:
        sys.path.insert(0, site_packages)


required_build_modules = [
    "aiofiles",
    "aiomysql",
    "aiosqlite",
    "click",
    "cryptography",
    "dotenv",
    "execjs",
    "httpx",
    "jieba",
    "openpyxl",
    "pandas",
    "parsel",
    "playwright",
    "pydantic",
    "humps",
    "redis",
    "sqlalchemy",
    "tenacity",
    "typer",
    "typing_extensions",
    "wordcloud",
]
missing_build_modules = [
    module_name
    for module_name in required_build_modules
    if importlib.util.find_spec(module_name) is None
]
if missing_build_modules:
    missing_modules_text = ", ".join(missing_build_modules)
    raise SystemExit(
        "PyInstaller build environment is missing required packages: "
        f"{missing_modules_text}\n"
        f"Current interpreter: {sys.executable}\n"
        "Run 'uv sync' first, then rebuild with '.\\.venv\\Scripts\\python.exe -m PyInstaller .\\MediaCrawler.spec --clean'."
    )

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
    'humps',
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
] + collect_submodules('typer') + collect_submodules('click')

# ==================== 分析 ====================
a = Analysis(
    ['bootstrap.py'],
    pathex=['.', *project_site_packages],
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
