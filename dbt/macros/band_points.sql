{% macro band_points(factor, value) %}
    {# The calling model must define a CTE named "bands" from the scoring_bands seed #}
    (select b.points
     from bands as b
     where b.factor = '{{ factor }}'
       and {{ value }} >= b.band_from
       and {{ value }} < b.band_to)
{% endmacro %}
