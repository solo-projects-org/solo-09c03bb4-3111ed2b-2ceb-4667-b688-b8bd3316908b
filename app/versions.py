"""目录版本对照：计算两个已留存版本之间的新增 / 删除 / 字段更正。

纯函数模块：输入两个版本的全部行，输出按震例号排序的差异清单。
调用方（main.py）在同一个只读事务里取出两个版本的行，因此对照期间
其他人留存新版本不会改变本次结果。
"""

FIELDS = ("origin_time", "latitude", "longitude", "magnitude")
FIELD_LABELS = {
    "origin_time": "发震时刻",
    "latitude": "纬度",
    "longitude": "经度",
    "magnitude": "震级",
}


def diff_catalogs(rows_a, rows_b):
    """对照两个目录版本。

    rows_a / rows_b：(event_id, origin_time, latitude, longitude, magnitude) 序列，
    分别来自旧版本 A 与新版本 B。

    返回 {
        "added":   [(event_id, origin_time, lat, lon, mag), ...],   # B 有 A 无
        "deleted": [(event_id, origin_time, lat, lon, mag), ...],   # A 有 B 无
        "changed": [(event_id, [(field, old, new), ...]), ...],     # 两边都有但字段不同
    }，全部按震例号排序。
    """
    a = {r[0]: tuple(r[1:]) for r in rows_a}
    b = {r[0]: tuple(r[1:]) for r in rows_b}
    added = [(eid,) + b[eid] for eid in sorted(b.keys() - a.keys())]
    deleted = [(eid,) + a[eid] for eid in sorted(a.keys() - b.keys())]
    changed = []
    for eid in sorted(a.keys() & b.keys()):
        diffs = [
            (FIELDS[i], a[eid][i], b[eid][i])
            for i in range(len(FIELDS))
            if a[eid][i] != b[eid][i]
        ]
        if diffs:
            changed.append((eid, diffs))
    return {"added": added, "deleted": deleted, "changed": changed}
