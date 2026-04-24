import httpx

SUPABASE_URL = "https://pwdammlneaclfbcywlow.supabase.co"
SUPABASE_KEY = "sb_publishable_5Opv_JbdYFzhfj1G3-W92g_TyXwcfWa"

def is_blocked() -> bool:
    try:
        r = httpx.get(
            f"{SUPABASE_URL}/rest/v1/system_config",
            params={"key": "eq.status", "select": "active"},
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            },
            timeout=3
        )
        data = r.json()
        if data and data[0].get("active") == False:
            return True
    except Exception:
        pass
    return False