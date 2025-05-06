--L’obiettivo di questo script è creare una vista materializzata costituita da quelle nazioni che rispettano i parametri da me imposti al fine di avere in output solo quei record i cui Stati possono essere classificati tra i più sviluppati al mondo. I parametri imposti sono: il pil pro capite, un’aspettativa di vita superiore a 75 anni, una popolazione urbana per ciascuna nazione maggiore di 4000000 di abitanti; inoltre, con INNER JOIN andremo a porre come ulteriore limite una percentuale di energia rinnovabile utilizzata sul totale del consumo di energia maggiore del 10%. 

CREATE MATERIALIZED VIEW biggest_nations AS
SELECT d.country, 
       d.urban_population, 
       d.life_expectancy, 
       d.gdp, 
       d.population, 
       d.gasoline_price, 
       d.minimum_wage, 
       d.unemployment_rate, 
       (d.gdp / d.population) AS pil_procapite, 
       AVG(e.renewable_energy_share_in_the_total_final_energy_consumption) AS mediautilizzo_energiarinnovabile_su_totaleenergia 
FROM dati_globali d
INNER JOIN dati_energia_sostenibile e ON d.country = e.country
WHERE d.urban_population > 4000000
  AND (d.gdp / d.population) > 20000
  AND d.life_expectancy > 75
GROUP BY d.country, 
         d.urban_population, 
         d.life_expectancy, 
         d.gdp, 
         d.population, 
         d.gasoline_price, 
         d.minimum_wage, 
         d.unemployment_rate
HAVING AVG(e.renewable_energy_share_in_the_total_final_energy_consumption) > 0.10
ORDER BY d.country ASC;


--obiettivo di questo script è quello di creare una View che fornisca una rappresentazione aggregata dei dati riguardanti la percentuale di energia rinnovabile e il prezzo della benzina per ciascun paese, oltre che la media del prezzo del gasolio e della percentuale di energia rinnovabile utilizzata per il campione di riferimento.
CREATE VIEW energia_rinnovabile_e_gasolio_medie_assolute AS
	SELECT 
		b.country, 
		b. mediautilizzo_energiarinnovabile_su_totaleenergia,
		b.gasoline_price,
		(SELECT AVG(gasoline_price) FROM biggest_nations) AS mediaassoluta_gasolio,
		(SELECT AVG(mediautilizzo_energiarinnovabile_su_totaleenergia) FROM biggest_nations) AS mediaassoluta_consumo_energiarinnovabile
	FROM biggest_nations b
GROUP BY
	b.country, b.mediautilizzo_energiarinnovabile_su_totaleenergia, b.gasoline_price
ORDER BY b.mediautilizzo_energiarinnovabile_su_totaleenergia ASC, b.gasoline_price;


--utilizziamo la view creata antecedenemente per calcolare la correlazione, tramite l’indice di correlazione di Pearson, tra il prezzo medio del gasolio e il consumo medio di energia rinnovabile sul totale di consumo energetico; intendo creare una CTE per il calcolo della correlazione:


WITH correlazioni AS (
    SELECT 
        CORR(mediaassoluta_consumo_energiarinnovabile, mediaassoluta_gasolio) AS correlazione_consumorinnovabile_e_gasolio
    FROM 
        energia_rinnovabile_e_gasolio_medie_assolute
)
SELECT 
    correlazione_consumorinnovabile_e_gasolio
FROM 
    correlazioni;


--con questo SELECT si vuole mostrare quali delle nazioni più sviluppate al mondo sfruttino in percentuale una maggior quantità di energia rinnovabile sul totale rispetto alle altre; viene utilizzato CASE WHEN così da ottenere, per ogni possibilità, il calcolo desiderato, ossia la differenza tra la media assolta e la media relativa.
SELECT 
    country,
    mediautilizzo_energiarinnovabile_su_totaleenergia,
    mediaassoluta_consumo_energiarinnovabile,
    CASE
        WHEN mediaassoluta_consumo_energiarinnovabile < mediautilizzo_energiarinnovabile_su_totaleenergia OR mediaassoluta_consumo_energiarinnovabile = mediautilizzo_energiarinnovabile_su_totaleenergia THEN mediautilizzo_energiarinnovabile_su_totaleenergia - mediaassoluta_consumo_energiarinnovabile
	ELSE mediaassoluta_consumo_energiarinnovabile - mediautilizzo_energiarinnovabile_su_totaleenergia
    END AS differenza
FROM energia_rinnovabile_e_gasolio_medie_assolute
GROUP BY 
    country,
    mediautilizzo_energiarinnovabile_su_totaleenergia,
   mediaassoluta_consumo_energiarinnovabile;

--qui si intende mostrare quanto il prezzo del gasolio per ogni nazione discosti dalla media assoluta
SELECT 
	country,
	gasoline_price,
	mediaassoluta_gasolio,
	CASE
		WHEN mediaassoluta_gasolio < gasoline_price OR mediaassoluta_gasolio = gasoline_price THEN gasoline_price - 	mediaassoluta_gasolio 		
	ELSE mediaassoluta_gasolio - gasoline_price
	END AS differenza
FROM energia_rinnovabile_e_gasolio_medie_assolute
GROUP BY 
	country,
	gasoline_price,
	mediaassoluta_gasolio;

--obiettivo di questo script è considerare altri parametri fondamentali in una nazione: stipendio minimo, pil pro capite e tasso di disoccupazione; per ognuna di questi parametri avremo una colonna in cui, per ciascuna nazione, si vedrà se il parametro di riferimento è sopra, sotto o uguale alla media di tutti gli stati del parametro stesso 
CREATE MATERIALIZED VIEW stipendi_minimi_disoccupazione_e_pilprocapite AS
	SELECT
		country,
		minimum_wage,
		unemployment_rate,
		pil_procapite,
		CASE
			WHEN minimum_wage > (SELECT AVG(minimum_wage) FROM biggest_nations) THEN 'above_average'
			WHEN minimum_wage < (SELECT AVG(minimum_wage) FROM biggest_nations) THEN 'under_average'
	    	WHEN minimum_wage IS NULL THEN NULL
			ELSE 'average'
		END AS wage,
		CASE
			WHEN unemployment_rate > (SELECT AVG(unemployment_rate) FROM biggest_nations) THEN 'above_average'
			WHEN unemployment_rate < (SELECT AVG(unemployment_rate) FROM biggest_nations) THEN 'under_average'
			WHEN unemployment_rate IS NULL THEN NULL
			ELSE 'average'
		END AS unemployment,
		CASE
			WHEN pil_procapite > (SELECT AVG(pil_procapite) FROM biggest_nations) THEN 'above_average'
			WHEN pil_procapite < (SELECT AVG(pil_procapite) FROM biggest_nations) THEN 'under_average'
			WHEN pil_procapite IS NULL THEN NULL
			ELSE 'average'
		END AS pil
	FROM biggest_nations
	ORDER BY country;

--ora creiamo una Vista che abbia come colonne ogni nazione e la rispettiva media di Co2 emesso dal 2000 al 2020; ci sarà utile per le successive operazioni; in questa view vogliamo considerare unicamente i paesi dell’america latina

CREATE VIEW media_emissioniCO2 AS
SELECT
    e.country,
    AVG(e.value_co2_emissions_kt_by_country) AS avg_emissioniCO2
FROM 
    dati_energia_sostenibile e
INNER JOIN dati_globali d ON e.country = d.country
	WHERE 
		d.official_language IN ('Spanish')
		AND d.country != 'Spain'	
		AND d.country != 'Equatorial Guinea'
GROUP BY 
    d.country,
	e.country;

--ora osserviamo l’indice di correlazione di Pearson tra il tasso di mortalità infantile e le emissioni di CO2 per i paesi dell’America latina di cui su si è creata la view
SELECT
    CORR(d.infant_mortality, m.avg_emissioniCO2) AS correlazione_mortalitàinfantile_CO2,
    CORR(d.fertility_rate, m.avg_emissioniCO2) AS correlazione_fertilita_CO2,
    CORR(d.infant_mortality, d.out_of_pocket_health_expenditure) AS correlazione_mortalitainfantile_spesesanitarie
FROM 
    dati_globali d
INNER JOIN  
    media_emissioniCO2 m
ON 
    d.country = m.country;



