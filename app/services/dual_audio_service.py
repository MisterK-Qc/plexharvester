# Remplacé par multi_audio_service.py
from .multi_audio_service import (  # noqa: F401
    get_multi_audio_status as get_dual_audio_status,
    start_multi_audio_job as start_dual_audio_job,
    check_multi_audio_deps as check_dual_audio_deps,
)
