from django.db import models


class TimeStampedModel(models.Model):
    """A model that tracks the creation and update times of a model"""

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class GenericAttribute(TimeStampedModel):
    """Represents a generic schema for attributes that are unstructured. These get tagged
    onto models, allowing for arbitrary metadata that the frontend can query and render.

    We're doing this so that we're not beholden too much by the schema, but we can easily store
    the commonalities. Plugins (on both the client and the FE) will define the types, which we intend to
    place in a centralized location.

    Note the design decision here -- we want to remain flexible and attach multiple attributes, for the following reasons:
    1. We may want to log more stuff over time
    2. We may want to log different stuff for different DAG types
    3. We might want to update the schema over time

    Take, for instance, the case of a NodeTemplate. We have a lot of information for Hamilton nodes that we want to store:
    - namespaces
    - whether its user-defined
    - the function location
    - tags
    - etc...

    We will very likely be changing it over time, and we don't want to have to conduct a migration
    as we do so. This design gives us the flexbility to store this data in a way that's easy to query
    (it'll be efficient enough as django automatically indexes on foreign keys), and we have flexibility.
    We can either store it all as a single json blog (E.G. a BaseHamiltonNode attribute type), or separate
    it out (or some combination).

    Furthermore, this design allows for fine-grained removal of information. Say, for instance, we
    accidentally store the code, and a customer wishes for us to remove it. Rather than looking through
    a JSON blob, we can delete the specific attributes.


    """

    name = models.CharField(max_length=63)
    type = models.CharField(max_length=63)
    schema_version = models.IntegerField()
    value = models.JSONField()

    class Meta:
        abstract = True
