{#
    This macro returns the full country name 
#}

{% macro get_full_country_name(code) -%}

    case "country-code"
        when 'at' then 'Austria'
        when 'ch' then 'Switzerland'
        when 'cz' then 'Czechia'        
        when 'de' then 'Germany'
        when 'fr' then 'France'
        when 'gb' then 'United Kingdom'      
        when 'it' then 'Italy'
        when 'lu' then 'Luxembourg'
        when 'nl' then 'The Netherlands'
        else 'Belgium'
    end

{%- endmacro %}