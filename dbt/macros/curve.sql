{% macro curve(factor, value) %}
    {# The calling model must define a CTE named "segments" built from the scoring_curves seed #}
    (select
        case
            when s.x1 is null then s.y0
            else s.y0 + (least(greatest({{ value }}, s.min_x), s.max_x) - s.x0)
                        * (s.y1 - s.y0) / (s.x1 - s.x0)
        end
     from segments as s
     where s.factor = '{{ factor }}'
       and {{ value }} is not null
       and least(greatest({{ value }}, s.min_x), s.max_x) >= s.x0
       and (least(greatest({{ value }}, s.min_x), s.max_x) < s.x1 or s.x1 is null))
{% endmacro %}
