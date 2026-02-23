{% macro repo_root() -%}
  {%- set root_path = env_var('EIDP_REPO_ROOT', '') | trim -%}

  {%- if not root_path -%}
    {%- set root_path = env_var('PWD', '') | trim -%}
  {%- endif -%}

  {%- if not root_path -%}
    {%- set project_dir = env_var('DBT_PROJECT_DIR', '') | trim -%}
    {%- if project_dir -%}
      {%- set root_path = modules.os.path.dirname(modules.os.path.dirname(project_dir)) -%}
    {%- endif -%}
  {%- endif -%}

  {%- if not root_path -%}
    {{ exceptions.raise_compiler_error(
      'Unable to resolve repository root. Set EIDP_REPO_ROOT, PWD, or DBT_PROJECT_DIR.'
    ) }}
  {%- endif -%}

  {%- set normalized = modules.os.path.normpath(root_path) -%}
  {{ return(normalized) }}
{%- endmacro %}
