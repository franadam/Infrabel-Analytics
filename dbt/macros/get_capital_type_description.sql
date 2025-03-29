{#
    This macro returns the description of the capital 
#}

{% macro get_capital_description(capital) -%}

    case REPLACE(capital, '"', '')
        when 'primary' then 'Country capital'
        when 'admin' then 'Administrative region capital'
        when 'minor' then 'Commune'
        else 'regular city/village'
    end

{%- endmacro %}