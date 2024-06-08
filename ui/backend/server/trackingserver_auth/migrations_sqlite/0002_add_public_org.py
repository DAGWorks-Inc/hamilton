from django.db import migrations
from trackingserver_auth.models import Team


def forwards(apps, schema_editor):
    if Team.objects.filter(name="Public").exists():
        return
    # This is purely due to backwards compatibility -- public was team #11 in the old repo
    public = Team(
        id=11,
        name="Public",
        auth_provider_organization_id="dummy",  # Dummy org -- this is (for now) going to be the only one of its kind
        auth_provider_type="public",
    )
    public.save()


class Migration(migrations.Migration):
    dependencies = [
        ("trackingserver_auth", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(forwards),
        migrations.RunSQL(
            "UPDATE sqlite_sequence SET seq = (SELECT MAX(id) FROM trackingserver_auth_team) WHERE name = 'trackingserver_auth_team';"
        ),
    ]
