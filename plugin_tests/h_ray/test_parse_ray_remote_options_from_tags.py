from hamilton.plugins import h_ray


def test_parse_ray_remote_options_from_tags():
    tags = {
        f"{h_ray.RAY_REMOTE_TAG_NAMESPACE}.resources": '{"GPU": 1}',
        "another_tag": "another_value",
    }

    ray_options = h_ray.parse_ray_remote_options_from_tags(tags)

    assert len(ray_options) == 1
    assert "resources" in ray_options
    assert ray_options["resources"] == {"GPU": 1}
