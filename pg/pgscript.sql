## SEARCH_PATH
SET search_path TO {0}, public;

## EFFACER_SCHEMA
DROP SCHEMA IF EXISTS {0} CASCADE;

## CREER_SCHEMA 
CREATE SCHEMA {0};

## CREER_SCHEMA_SI_INEXISTANT 
CREATE SCHEMA IF NOT EXISTS {0};

## EFFACER_TABLE
DROP TABLE IF EXISTS {0}.{1} CASCADE;

## RENOMMER_TABLE
ALTER TABLE {0}.{1} RENAME To {2};

## EFFACER_ET_CREER_SEQUENCE
DROP SEQUENCE IF EXISTS {0}.{1};
CREATE SEQUENCE {0}.{1};

## LISTER_SCHEMAS::list
SELECT nspname
FROM pg_catalog.pg_namespace 

## LISTER_SCHEMAS_COMMENCANT_PAR::list 
SELECT nspname
FROM pg_catalog.pg_namespace 
WHERE nspname LIKE '{0}%';

## LISTER_SCHEMAS_CONTENANT::list 
SELECT nspname
FROM pg_catalog.pg_namespace 
WHERE nspname LIKE '%{0}%';

## LISTER_TABLES::list
SELECT tablename 
FROM pg_tables 
WHERE schemaname = '{0}';

## LISTER_TABLES_COMMENCANT_PAR::list 
SELECT tablename 
FROM pg_tables 
WHERE schemaname = '{0}' 
AND tablename LIKE '{1}%';

## LISTER_CHAMPS::normal
SELECT 
	ordinal_position as num, 
	column_name as nom, 
	CASE 
		WHEN data_type IN ('character varying', 'character')  AND character_maximum_length IS NOT NULL THEN data_type || '(' || character_maximum_length || ')' 
		WHEN data_type = 'ARRAY' THEN
			CASE 
				WHEN udt_name = '_text' then 'TEXT[]'
				WHEN udt_name = '_int4' THEN 'INTEGER[]'
				WHEN udt_name = '_varchar' THEN 'CHARACTER VARYING[]'
				WHEN udt_name = '_numeric' THEN 'NUMERIC[]'
				WHEN udt_name = '_date' THEN 'DATE[]'
			END
		WHEN data_type = 'USER-DEFINED' THEN 'geometry'
		ELSE data_type 
	END AS typ
FROM information_schema.columns
WHERE table_schema = '{0}'
AND table_name = '{1}'
ORDER BY ordinal_position;

## _AJOUTER_CLEF_PRIMAIRE
ALTER TABLE {0}.{1} 
ADD CONSTRAINT {1}_pkey PRIMARY KEY ({2});

## _AJOUTER_CONTRAINTE_UNICITE
ALTER TABLE {0}.{1} 
ADD CONSTRAINT {1}_unique UNIQUE ({2});

## AJOUTER_CONTRAINTE_CHECK
ALTER TABLE {0}.{1} 
ADD CONSTRAINT {1}_check CHECK ({2});

## _AJOUTER_COMMENTAIRE_SUR_CHAMP
COMMENT ON COLUMN {0}.{1}.{2} IS '{3}';

## _AJOUTER_COMMENTAIRE_SUR_TABLE
COMMENT ON TABLE {0}.{1} IS '{2}';

## MODIFIER_TYPE_COLONNE
ALTER TABLE {0}.{1}
ALTER COLUMN {2} SET DATA TYPE {3};

## COPIER_TABLE
DROP TABLE IF EXISTS {2}.{3} CASCADE;
CREATE TABLE {2}.{3} AS (SELECT * FROM {0}.{1});

## CHANGER_SCHEMA
ALTER TABLE {0}.{1} SET SCHEMA {2};

## COMPTER
SELECT count(*) FROM {0}.{1};

## LISTER_VALEURS::list
SELECT {0} FROM {1}.{2};

## LISTER_VALEURS_DISTINCTES::list
SELECT DISTINCT {0} FROM {1}.{2};

## CREER_EXTENSION_FDW
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

## CREER_SERVEUR_DISTANT_FDW
CREATE SERVER {3}
FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS (host '{0}', dbname '{1}', port '{2}');

## EFFACER_SERVEUR_DISTANT_FDW
DROP SERVER IF EXISTS {0} CASCADE;

## CREER_USER_MAPPING_POUR_SERVEUR_DISTANT_FDW
CREATE USER MAPPING FOR CURRENT_USER SERVER {1} OPTIONS (user '{0}', password '{2}');

## EFFACER_USER_MAPPING_POUR_SERVEUR_DISTANT_FDW
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER {1};

## EFFACER_TABLE_ETRANGERE
DROP FOREIGN TABLE IF EXISTS {0}.{1};

## _CREER_TABLE_ETRANGERE
CREATE FOREIGN TABLE {0}.{1} 
(
{4}    
)SERVER {5}
OPTIONS (schema_name '{2}', table_name '{3}')

## CREER_FONCTION_ARRAY_SUPPRIMER_NULL
CREATE OR REPLACE FUNCTION array_supprimer_null(numlot VARCHAR[])
  RETURNS VARCHAR[] AS
$BODY$
/*
   Retourne un tableau en enlevant les valeurs NULL (à partir de la version 9.3, devient obsolète avec apparition de array_remove)
  */
DECLARE    
    lot VARCHAR[];
    resultat VARCHAR[];
BEGIN
        lot := numlot;        
        FOR i IN 1..array_upper(lot,1)
        LOOP
            IF lot[i] IS NOT NULL THEN resultat := resultat || lot[i]; END IF;        
        END LOOP;
        RETURN resultat;
END;
$BODY$
  LANGUAGE plpgsql;

## CREER_FONCTION_PGCD
CREATE OR REPLACE FUNCTION pgcd(valeurs integer[])
  RETURNS integer AS
$BODY$
/*
  Retourne le Plus Grand Commun Diviseur de l'ensemble des valeurs du tableau d'entier.
  Si le tableau ne contient qu'un seul entier, Retourne sa valeur absolue.
 
  Exemple : SELECT pgcd(ARRAY[14,28,49,70]);
  7
 */
DECLARE
    v integer[];
    reste integer;
    a integer;
    b integer;
    gcd integer;
BEGIN
    v := valeurs;
    gcd := 0;
    IF array_length(v,1) > 1 THEN        
        a := v[1];    
        FOR i IN 2..array_upper(v,1)
        LOOP         
            b:=v[i];
            IF b<>0 THEN 
                LOOP
                    reste:=a%b;
                    a:=b;
                    b:=reste;
                    EXIT WHEN reste=0;
                END LOOP;
                
                IF abs(a) < gcd OR gcd = 0 THEN 
                    gcd := abs(a);
                    a := gcd;
                END IF;
                EXIT WHEN gcd=1;
            ELSE
                gcd=0;
                EXIT;
            END IF;    
        END LOOP;
    ELSE
        gcd = abs(v[1]);
    END IF;
        RETURN gcd;
END;
$BODY$
  LANGUAGE plpgsql; 

## CREER_FONCTION_CONVERTIR_DATE
CREATE OR REPLACE FUNCTION convertir_date(text)
RETURNS date AS
$BODY$
/*
Convertit une chaine de caractère JJMMAAAA en date

exemples : 
SELECT convertir_date('31122012');
>> 2012-12-31
*/

DECLARE 
	jour VARCHAR;
	mois VARCHAR;
	annee VARCHAR;
BEGIN
IF $1 ~ '^[0-9]{{8}}$' THEN
	jour := substring($1,1,2);
	mois := substring($1,3,2);
	annee := SUBSTRING($1,5,4);
	return ( annee || '-' || mois || '-' || jour)::Date;
ELSE
	RETURN NULL;
END IF;
END;
$BODY$
LANGUAGE plpgsql;

## CREER_FONCTION_AGGREGAT_FUSION_ARRAY
DROP AGGREGATE IF EXISTS fusion_array(BIGINT[]);
CREATE AGGREGATE fusion_array(BIGINT[])(sfunc = array_cat, stype = BIGINT[]);
DROP AGGREGATE IF EXISTS fusion_array(VARCHAR[]);
CREATE AGGREGATE fusion_array(VARCHAR[])(sfunc = array_cat, stype = VARCHAR[]);
DROP AGGREGATE IF EXISTS fusion_array(INTEGER[]);
CREATE AGGREGATE fusion_array(INTEGER[])(sfunc = array_cat, stype = INTEGER[]);

## CREER_FONCTION_ARRAY_SORT_UNIQUE
CREATE OR REPLACE FUNCTION array_sort_unique (ANYARRAY) RETURNS ANYARRAY
LANGUAGE SQL
AS $BODY$
  SELECT ARRAY(
	SELECT DISTINCT $1[s.i]
	FROM generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)
	ORDER BY 1
  );
$BODY$;

## CREER_FONCTION_AGGREGAT_FIRST
-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$; 
-- And then wrap an aggregate around it
DROP AGGREGATE IF EXISTS first(anyelement);
CREATE AGGREGATE FIRST (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
);
