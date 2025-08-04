import os
try:
    import win32api
except ImportError:
    print("⚠️ Biblioteca pywin32 não encontrada. Instale com: pip install pywin32")

def obter_versao_exe(caminho_exe):
    """
    Retorna a versão do executável .exe fornecido.
    """
    if not os.path.exists(caminho_exe):
        print(f"⚠️ Caminho não encontrado: {caminho_exe}")
        return "0.0.0.0"
    try:
        info = win32api.GetFileVersionInfo(caminho_exe, '\\')
        ms = info['FileVersionMS']
        ls = info['FileVersionLS']
        return f"{ms >> 16}.{ms & 0xFFFF}.{ls >> 16}.{ls & 0xFFFF}"
    except Exception as e:
        print(f"⚠️ Erro ao obter versão de {caminho_exe}: {e}")
        return "0.0.0.0"
