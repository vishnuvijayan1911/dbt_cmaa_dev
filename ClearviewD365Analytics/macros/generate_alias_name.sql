{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {# default alias is the model filename unless explicitly set #}
  {%- set default_alias = custom_alias_name or node.name -%}
  {%- set src = node.meta.get('source_system') -%}

  {%- if src %}
    {{ src }}_{{ default_alias }}
  {%- else %}
    {{ default_alias }}
  {%- endif %}
{%- endmacro %}
