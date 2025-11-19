{% macro generate_schema_name(custom_schema_name, node) %}
  {% if custom_schema_name is none %}
    return(target.schema)
  {% else %}
    return(custom_schema_name)   -- e.g., 'bronze', 'silver', 'gold'
  {% endif %}
{% endmacro %}
{# ------------------------------------------------------------------------------
  Custom schema generator
  Honors model-level `+schema` configs (bronze/silver/gold) and falls back
  to the target schema when no override is provided.
------------------------------------------------------------------------------ #}
{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {# Use the schema explicitly defined on the model/config when present. #}
  {%- if custom_schema_name is not none and custom_schema_name|length > 0 -%}
    {{ custom_schema_name }}
  {%- else -%}
    {# Otherwise rely on the profile/environment's default schema. #}
    {{ target.schema }}
  {%- endif -%}
{%- endmacro %}
