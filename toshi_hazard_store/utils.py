"""Common utilities."""

from nzshm_common.location.code_location import CodedLocation


def normalise_site_code(oq_site_object: tuple, force_normalized: bool = False) -> CodedLocation:
    """Return a valid code for storage."""

    if len(oq_site_object) not in [2, 3]:
        raise ValueError(f"Unknown site object {oq_site_object}")

    force_normalized = force_normalized if len(oq_site_object) == 3 else True

    if len(oq_site_object) == 3:
        _, lon, lat = oq_site_object
    elif len(oq_site_object) == 2:
        lon, lat = oq_site_object

    rounded = CodedLocation(lon=lon, lat=lat, resolution=0.001)

    if not force_normalized:
        rounded.code = oq_site_object[0].decode()  # restore the original location code
    return rounded
