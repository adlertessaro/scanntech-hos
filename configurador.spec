# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['C:\\Users\\AdyFera\\Documents\\Scanntech\\scanntech\\config\\configurador.py'],
    pathex=[],
    binaries=[],
    datas=[('C:\\MercFarma\\Integracoes\\Integradores\\HOS.Integracoes.Scanntech\\settings.config', '.')],
    hiddenimports=['psycopg2', 'ttkbootstrap', 'pycryptodome', 'cryptography'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='configurador',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['C:\\Users\\AdyFera\\Documents\\Scanntech\\scanntech\\core\\icon.ico'],
)
