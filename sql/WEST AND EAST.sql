--1.L’obiettivo di questa query è creare una vista materializzata che combina dati demografici, economici e energetici per i paesi arabi. Con questa vista si aggregano i tassi medi di energia rinnovabile e consumo energetico per facilitare le successive analisi riguardo questi paesi.
CREATE MATERIALIZED VIEW arab_nations_energy AS
SELECT 
    e.country, 
    e.urban_population, 
    e.life_expectancy, 
    e.gdp, 
    e.population, 
    e.gasoline_price, 
    e.minimum_wage, 
    e.unemployment_rate, 
    (e.gdp / e.population) AS gdp_per_capita,
    AVG(s.renewable_energy_share_in_the_total_final_energy_consumption) AS avg_renewable_energy_share,
    AVG(s.primary_energy_consumption_per_capita_kwh_per_person) AS avg_energy_consumption
FROM dati_globali e
INNER JOIN dati_energia_sostenibile s ON e.country = s.country
WHERE e.official_language IN ('Arabic')
GROUP BY e.country, 
         e.urban_population, 
         e.life_expectancy, 
         e.gdp, 
         e.population, 
         e.gasoline_price, 
         e.minimum_wage, 
         e.unemployment_rate
ORDER BY e.country;

--2. L'obiettivo di questa query è classificare i paesi arabi in base alla loro adozione dell'energia rinnovabile rispetto alla media regionale, identificando quali paesi sono al di sopra o sotto la media.
SELECT 
    country,
    avg_renewable_energy_share,
    CASE
		WHEN avg_renewable_energy_share IS NULL THEN NULL
        WHEN avg_renewable_energy_share > (SELECT AVG(avg_renewable_energy_share) FROM arab_nations_energy) THEN 'above_average'
        ELSE 'below_average'
    END AS renewable_energy_status
FROM arab_nations_energy
ORDER BY avg_renewable_energy_share DESC;

--3.L'obiettivo di questa query è analizzare quale sia il rapporto tra il consumo energetico medio e il tasso di disoccupazione nei paesi arabi, classificandoli in base al loro tasso di disoccupazione rispetto alla media regionale.
SELECT 
    country,
    avg_energy_consumption,
    unemployment_rate,
    CASE
        WHEN unemployment_rate > (SELECT AVG(unemployment_rate) FROM arab_nations_energy) THEN 'above_average'
        ELSE 'below_average'
    END AS unemployment_status
FROM arab_nations_energy
ORDER BY unemployment_rate DESC;

--4.L'obiettivo di questa query è calcolare la correlazione che intercorre tra il PIL pro capite e l'aspettativa di vita, e tra il consumo energetico medio e il PIL pro capite nei paesi arabi, così da identificare eventuali relazioni tra gli indicatori.
SELECT 
    CORR(gdp_per_capita, life_expectancy) AS correlation_gdp_life_expectancy, 
    CORR(avg_energy_consumption, gdp_per_capita) AS correlation_energy_gdp
FROM arab_nations_energy;

--l'obiettivo di questa query è quello di creare una vista materializzata per calcolare le medie dei valori di salario minimo, tasso di disoccupazione e PIL pro capite per i paesi arabi.

CREATE MATERIALIZED VIEW avg_values AS
SELECT 
    AVG(minimum_wage) AS avg_minimum_wage, 
    AVG(unemployment_rate) AS avg_unemployment_rate, 
    AVG(gdp / population) AS avg_gdp_per_capita
FROM dati_globali
WHERE official_language IN ('Arabic');




--5.L'obiettivo di questa query è creare una vista materializzata che classifica i paesi arabi in base al salario minimo, al tasso di disoccupazione e al PIL pro capite rispetto alla media regionale. Questa vista serve per facilitare ulteriori analisi comparative.
CREATE MATERIALIZED VIEW arab_nations_avg_status AS
WITH avg_values AS (
    SELECT 
        AVG(minimum_wage) AS avg_minimum_wage, 
        AVG(unemployment_rate) AS avg_unemployment_rate, 
        AVG(gdp / population) AS avg_gdp_per_capita
    FROM dati_globali
    WHERE official_language IN ('Arabic')
)
SELECT 
    d.country,
    d.minimum_wage,
    d.unemployment_rate,
    (d.gdp / d.population) AS gdp_per_capita,
    CASE
		WHEN d.minimum_wage IS NULL THEN NULL
        WHEN d.minimum_wage > a.avg_minimum_wage THEN 'above_average'
        WHEN d.minimum_wage < a.avg_minimum_wage THEN 'below_average'
        ELSE 'average'
    END AS wage_status,
    CASE
		WHEN d.unemployment_rate IS NULL THEN NULL
        WHEN d.unemployment_rate > a.avg_unemployment_rate THEN 'above_average'
        WHEN d.unemployment_rate < a.avg_unemployment_rate THEN 'below_average'
        ELSE 'average'
    END AS unemployment_status,
    CASE
        WHEN (d.gdp / d.population) > a.avg_gdp_per_capita THEN 'above_average'
        WHEN (d.gdp / d.population) < a.avg_gdp_per_capita THEN 'below_average'
        ELSE 'average'
    END AS gdp_status
FROM dati_globali d
LEFT JOIN avg_values a ON TRUE
WHERE d.official_language IN ('Arabic')
ORDER BY d.country;

--6.L'obiettivo di questa query è calcolare le emissioni cumulative di CO2 negli Emirati Arabi Uniti anno per anno, nonché la media delle emissioni di CO2, così da monitorare l'andamento delle emissioni nel tempo.
SELECT 
    country, 
    year, 
    value_co2_emissions_kt_by_country,
    SUM(value_co2_emissions_kt_by_country) OVER (PARTITION BY country ORDER BY year) AS cumulative_co2_emissions,
    (SELECT AVG(value_co2_emissions_kt_by_country) 
     FROM dati_energia_sostenibile 
     WHERE country = 'United Arab Emirates') AS avg_co2_emissions_uae
FROM dati_energia_sostenibile
WHERE country = 'United Arab Emirates'
ORDER BY year;

--7.L'obiettivo di questa query è creare una vista materializzata che calcola le emissioni cumulative di CO2 per i principali paesi occidentali selezionati. Questa vista facilita confronti e analisi delle emissioni di CO2 tra paesi.
CREATE MATERIALIZED VIEW western_nations_co2 AS
SELECT 
    country,
    year,
    value_co2_emissions_kt_by_country,
    SUM(value_co2_emissions_kt_by_country) OVER (PARTITION BY country ORDER BY year) AS cumulative_co2_emissions
FROM dati_energia_sostenibile
WHERE country IN ('United States', 'Germany', 'France', 'United Kingdom', 'Italy')
ORDER BY country, year;

--8.L'obiettivo di questa query è creare una vista materializzata che calcola le emissioni cumulative di CO2 per gli Emirati Arabi Uniti. Questa vista facilita il confronto delle emissioni di CO2 con quelle dei paesi occidentali.
CREATE MATERIALIZED VIEW uae_co2 AS
SELECT 
    country,
    year,
    value_co2_emissions_kt_by_country,
    SUM(value_co2_emissions_kt_by_country) OVER (PARTITION BY country ORDER BY year) AS cumulative_co2_emissions
FROM dati_energia_sostenibile
WHERE country = 'United Arab Emirates'
ORDER BY country, year;

--9.L'obiettivo di questa query è confrontare le emissioni medie e cumulative di CO2 tra i principali paesi occidentali e gli Emirati Arabi Uniti per valutare le differenze tra queste regioni.
SELECT 
    country,
    AVG(value_co2_emissions_kt_by_country) AS avg_co2_emissions,
    MAX(cumulative_co2_emissions) AS total_cumulative_co2_emissions
FROM western_nations_co2
GROUP BY country
UNION ALL
SELECT 
    country,
    AVG(value_co2_emissions_kt_by_country) AS avg_co2_emissions,
    MAX(cumulative_co2_emissions) AS total_cumulative_co2_emissions
FROM uae_co2
GROUP BY country;

--10.L'obiettivo di questa query è creare una vista materializzata che aggrega il PIL medio, le emissioni medie di CO2 e le emissioni cumulative di CO2 per i principali paesi occidentali e gli Emirati Arabi Uniti, in modo da facilitare l'analisi della relazione tra economia e ambiente.
CREATE MATERIALIZED VIEW gdp_co2 AS
SELECT 
    g.country,
    AVG(g.gdp) AS avg_gdp,
    AVG(e.value_co2_emissions_kt_by_country) AS avg_co2_emissions,
    SUM(e.value_co2_emissions_kt_by_country) AS cumulative_co2_emissions
FROM dati_globali g
INNER JOIN dati_energia_sostenibile e ON g.country = e.country
WHERE g.country IN ('United Arab Emirates', 'United States', 'Germany', 'France', 'United Kingdom', 'Italy')
GROUP BY g.country
ORDER BY g.country;

--11.L'obiettivo di questa query è estrarre dalla vista gdp_co2 i valori medi e cumulativi del PIL e delle emissioni di CO2 per ciascun paese, ordinati per paese, fornendo un'analisi comparativa tra i paesi selezionati.
SELECT 
    country,
    avg_gdp,
    avg_co2_emissions,
    cumulative_co2_emissions
FROM gdp_co2
ORDER BY country;

--12.L'obiettivo di questa query è creare una vista materializzata che raccoglie i dati annuali sulla quota di energia rinnovabile nel consumo energetico totale per i principali paesi occidentali e per i paesi arabi. Questa vista consente l'analisi dei tassi di adozione dell'energia rinnovabile.
CREATE MATERIALIZED VIEW energy_adoption AS
SELECT 
    e.country,
    e.year,
    e.renewable_energy_share_in_the_total_final_energy_consumption
FROM dati_energia_sostenibile e
WHERE e.country IN ('United Arab Emirates', 'United States', 'Germany', 'France', 'United Kingdom', 'Italy')
   OR e.country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
ORDER BY e.country, e.year;

--13.L'obiettivo di questa query è calcolare la media dei tassi di adozione dell'energia rinnovabile per ciascun paese, ordinati in ordine decrescente; si identificherà quali paesi adottino maggiormente l'energia rinnovabile.
SELECT 
    country,
    AVG(renewable_energy_share_in_the_total_final_energy_consumption) AS avg_renewable_energy_share
FROM energy_adoption
GROUP BY country
ORDER BY avg_renewable_energy_share DESC;

--14.L'obiettivo di questa query è confrontare i tassi medi di adozione dell'energia rinnovabile tra i paesi arabi e i paesi occidentali, raggruppando i risultati per regione e ordinandoli per la quota media di energia rinnovabile.
SELECT 
    'Arab Countries' AS region,
    country,
    AVG(renewable_energy_share_in_the_total_final_energy_consumption) AS avg_renewable_energy_share
FROM energy_adoption
WHERE country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
GROUP BY country
UNION ALL
SELECT 
    'Western Countries' AS region,
    country,
    AVG(renewable_energy_share_in_the_total_final_energy_consumption) AS avg_renewable_energy_share
FROM energy_adoption
WHERE country IN ('United States', 'Germany', 'France', 'United Kingdom', 'Italy')
GROUP BY country
ORDER BY region, avg_renewable_energy_share DESC;

--15.L'obiettivo di questa query è creare una vista materializzata che combina i dati sulle emissioni di CO2 e sui tassi di energia rinnovabile per ciascun paese e anno, consentendo di analizzare la relazione tra l'adozione dell'energia rinnovabile e le emissioni di CO2.
CREATE MATERIALIZED VIEW full_join_energy_emissions AS
SELECT 
    e.country,
    e.year,
    e.renewable_energy_share_in_the_total_final_energy_consumption,
    c.value_co2_emissions_kt_by_country
FROM dati_energia_sostenibile e
FULL JOIN dati_energia_sostenibile c ON e.country = c.country AND e.year = c.year
ORDER BY e.country, e.year;

--16. L'obiettivo di questa query è fornire una visione d'insieme dei principali indicatori economici e di accesso ai servizi energetici per i paesi arabi, concentrandosi su PIL pro capite, accesso all'elettricità e utilizzo di combustibili puliti per cucinare.
SELECT 
    e.country,
    AVG(g.gdp / g.population) AS avg_gdp_per_capita,
    AVG(e.access_to_electricity_percent_of_population) AS avg_access_to_electricity,
    AVG(e.access_to_clean_fuels_for_cooking) AS avg_access_to_clean_fuels
FROM dati_energia_sostenibile e
LEFT JOIN dati_globali g ON e.country = g.country
WHERE e.country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
GROUP BY e.country
ORDER BY avg_gdp_per_capita DESC, avg_access_to_electricity DESC;

--17.L'obiettivo di questa query è creare una vista materializzata che raccoglie i dati annuali sul consumo energetico pro capite per i principali paesi occidentali e per i paesi arabi. Questa vista consente di analizzare le differenze nel consumo energetico tra queste regioni.
CREATE MATERIALIZED VIEW energy_consumption_per_capita AS
SELECT 
    e.country,
    e.year,
    e.primary_energy_consumption_per_capita_kwh_per_person
FROM dati_energia_sostenibile e
WHERE e.country IN ('United Arab Emirates', 'United States', 'Germany', 'France', 'United Kingdom', 'Italy')
   OR e.country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
ORDER BY e.country, e.year;

--18.L'obiettivo di questa query è confrontare la media del consumo energetico pro capite tra i paesi arabi e i paesi occidentali, raggruppando i risultati per regione e ordinandoli per consumo energetico medio per persona
SELECT 
    'Arab Countries' AS region,
    country,
    AVG(primary_energy_consumption_per_capita_kwh_per_person) AS avg_energy_consumption_per_capita
FROM energy_consumption_per_capita
WHERE country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
GROUP BY country
UNION ALL
SELECT 
    'Western Countries' AS region,
    country,
    AVG(primary_energy_consumption_per_capita_kwh_per_person) AS avg_energy_consumption_per_capita
FROM energy_consumption_per_capita
WHERE country IN ('United States', 'Germany', 'France', 'United Kingdom', 'Italy')
GROUP BY country
ORDER BY region, avg_energy_consumption_per_capita DESC;

--19. L'obiettivo di questa query è creare una vista materializzata che calcola la percentuale della popolazione urbana per i principali paesi occidentali e per i paesi arabi. Questa vista consente di analizzare il livello di urbanizzazione.
CREATE MATERIALIZED VIEW urban_population_percentage AS
SELECT 
    country,
    (urban_population / population) * 100 AS urban_population_percentage
FROM dati_globali
WHERE country IN ('United Arab Emirates', 'United States', 'Germany', 'France', 'United Kingdom', 'Italy')
   OR country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
ORDER BY country;

--20.L'obiettivo di questa query è creare una vista materializzata che combina i dati sul consumo energetico pro capite e le emissioni di CO2 per ciascun paese e anno. Questa vista consente di analizzare la relazione tra il consumo energetico e le emissioni di CO2.
CREATE MATERIALIZED VIEW energy_vs_co2 AS
SELECT 
    e.country,
    e.year,
    e.primary_energy_consumption_per_capita_kwh_per_person,
    c.value_co2_emissions_kt_by_country
FROM dati_energia_sostenibile e
INNER JOIN dati_energia_sostenibile c ON e.country = c.country AND e.year = c.year
WHERE e.country IN ('United Arab Emirates', 'United States', 'Germany', 'France', 'United Kingdom', 'Italy')
   OR e.country IN (SELECT country FROM dati_globali WHERE official_language = 'Arabic')
ORDER BY e.country, e.year;

--21.L'obiettivo di questa query è calcolare la correlazione tra il consumo energetico pro capite e le emissioni di CO2 per ciascun paese. Questo aiuta a identificare se esiste una relazione significativa tra questi due indicatori.
SELECT 
    country,
    CORR(primary_energy_consumption_per_capita_kwh_per_person, value_co2_emissions_kt_by_country) AS correlation_energy_co2
FROM energy_vs_co2
GROUP BY country;