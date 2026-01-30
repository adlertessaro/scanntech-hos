import os
import logging

def obter_versao_exe(caminho_exe):
    """
    Obtém a versão de um arquivo executável Windows (.exe).
    Retorna a versão no formato string (ex: '8.5.2.1') ou uma versão padrão se falhar.
    """
    if not os.path.exists(caminho_exe):
        logging.warning(f"⚠️ Arquivo não encontrado: {caminho_exe}")
        return "1.0.0.0"
    
    try:
        # Tenta importar win32api (pywin32)
        import win32api
        
        # Obter informações de versão do arquivo
        info = win32api.GetFileVersionInfo(caminho_exe, "\\")
        ms = info['FileVersionMS']
        ls = info['FileVersionLS']
        
        # Converter para formato legível
        versao = f"{win32api.HIWORD(ms)}.{win32api.LOWORD(ms)}.{win32api.HIWORD(ls)}.{win32api.LOWORD(ls)}"
        
        logging.debug(f"✅ Versão obtida de {caminho_exe}: {versao}")
        return versao
        
    except ImportError:
        logging.error(f"❌ Biblioteca 'pywin32' não instalada. Instale com: pip install pywin32")
        return "1.0.0.0"
        
    except Exception as e:
        logging.error(f"❌ Erro ao obter versão de {caminho_exe}: {e}")
        return "1.0.0.0"
