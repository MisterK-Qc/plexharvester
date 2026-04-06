import base64
import re


def register_filters(app):
    @app.template_filter("b64encode")
    def b64encode_filter(value):
        if not value:
            return ""
        return base64.b64encode(value.encode("utf-8")).decode("utf-8")

    @app.template_filter("underscore_to_dash")
    def underscore_to_dash(value):
        try:
            s = str(value)
        except Exception:
            return value
        return re.sub(r'_(?=.)', ' - ', s)

    def get_resolution_badge(res):
        res = (res or '').lower()
        if '4k' in res:
            return '<span class="badge uhd">4K</span>'
        elif '1080' in res:
            return '<span class="badge fullhd">1080p</span>'
        elif '720' in res:
            return '<span class="badge hd">720p</span>'
        elif '480' in res or 'sd' in res:
            return '<span class="badge sd">SD</span>'
        return ''

    def get_codec_badge(codec):
        if not codec:
            return '<span class="badge codec badge-unknown">N/A</span>'
        lower = codec.lower()
        mapping = {
            'h264': 'h264',
            'avc': 'h264',
            'h265': 'h265',
            'hevc': 'hevc',
            'av1': 'av1',
            'vp9': 'vp9',
            'mpeg4': 'mpeg4',
            'xvid': 'xvid',
            'divx': 'divx'
        }
        class_suffix = mapping.get(lower, 'unknown')
        return f'<span class="badge codec badge-{class_suffix}">{codec.upper()}</span>'

    def get_bitrate_badge(bitrate):
        if not bitrate or not isinstance(bitrate, (int, float)) or bitrate == 0:
            return '<span class="badge bitrate bitrate-unk">N/A</span>'
        mbps = bitrate / 1000
        level = 'bitrate-unk'
        if mbps >= 10:
            level = 'bitrate-high'
        elif mbps >= 5:
            level = 'bitrate-med'
        else:
            level = 'bitrate-low'
        return f'<span class="badge bitrate {level}">{mbps:.1f} Mbps</span>'

    app.jinja_env.globals.update(getResolutionBadge=get_resolution_badge)
    app.jinja_env.globals.update(getCodecBadge=get_codec_badge)
    app.jinja_env.globals.update(getBitrateBadge=get_bitrate_badge)