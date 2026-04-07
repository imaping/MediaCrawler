import os
import sys
if getattr(sys, 'frozen', False):
    _exe_dir = os.path.dirname(sys.executable)
    _meipass = sys._MEIPASS
    import builtins
    _original_open = builtins.open
    _resource_prefixes = ('libs/', 'libs\\', 'docs/', 'docs\\', './docs/', '.\\docs\\')
    def _patched_open(file, *args, **kwargs):
        if isinstance(file, str) and file.startswith(_resource_prefixes):
            meipass_path = os.path.join(_meipass, file.lstrip('./'))
            if os.path.exists(meipass_path):
                return _original_open(meipass_path, *args, **kwargs)
        return _original_open(file, *args, **kwargs)
    builtins.open = _patched_open
    os.chdir(_exe_dir)