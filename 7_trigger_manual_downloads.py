import requests


def trigger_download(mls_id: int, mls_id_str: str, content_type: str, content_sub_type: str) -> bool:
    """Triggers download for a single MLS and content type combination."""
    url = f"{PROD_BASE_URL}/v1/listing-mflow/reprocess/download/mls/{mls_id}"

    params = {
        "dl_type": "range",
        "relative_period": DEFAULT_RELATIVE_PERIOD,
        "relative_period_unit": DEFAULT_RELATIVE_PERIOD_UNIT,
        "listing_id_name": "ListingId",
        "use_in_operator": "true",
        "content_type": content_type,
        "content_sub_type": content_sub_type,
        "query_validation": "false",
        "force": "true",
    }

    logger.info(f"🚀 Triggering 3-Day Download | MLS: {mls_id} ({mls_id_str}) | [{content_type} / {content_sub_type}]")

    try:
        response = requests.post(url, headers=HEADERS, params=params, data="", timeout=30)

        # 200 SUCCESS - Capture Batch ID
        if response.status_code == 200:
            data = response.json()
            batch_id = data.get("batch_id", "N/A")
            logger.info(f"   ✅ SUCCESS [200]: Triggered Batch ID = {batch_id}")
            return True

        # 403 AUTH FAILURE - Fatal, stop loop
        elif response.status_code == 403:
            err = response.json().get("message", response.text)
            logger.error(f"   ❌ AUTH ERROR [403]: Invalid API key. ({err})")
            return False

        # 406 / 422 INPUT VALIDATION ERRORS
        elif response.status_code in (406, 422):
            err = response.json().get("message", response.text)
            logger.error(f"   ❌ INPUT ERROR [{response.status_code}]: Invalid request for MLS {mls_id} ({content_type}/{content_sub_type}). Message: {err}")
            return False

        # OTHER UNEXPECTED ERRORS
        else:
            logger.error(f"   ❌ UNEXPECTED ERROR [{response.status_code}]: {response.text}")
            return False

    except requests.RequestException as e:
        logger.error(f"   ❌ Request Exception: {e}", exc_info=True)
        return False
