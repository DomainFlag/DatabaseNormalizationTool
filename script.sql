DROP TABLE IF EXISTS elections;

CREATE TABLE elections (
    "Code du département" DECIMAL NOT NULL,
    "Département" VARCHAR(255) NOT NULL,
    "Code de la circonscription" DECIMAL NOT NULL,
    "Circonscription" VARCHAR(255) NOT NULL,
    "Code de la commune" DECIMAL NOT NULL,
    "Commune" VARCHAR(255) NOT NULL,
    "Bureau de vote" TEXT NOT NULL,
    "Inscrits" DECIMAL NOT NULL,
    "Abstentions" DECIMAL NOT NULL,
    "% Abs/Ins" DECIMAL NOT NULL,
    "Votants" DECIMAL NOT NULL,
    "% Vot/Ins" DECIMAL NOT NULL,
    "Blancs" DECIMAL NOT NULL,
    "% Blancs/Ins" DECIMAL NOT NULL,
    "% Blancs/Vot" DECIMAL NOT NULL,
    "Nuls" DECIMAL NOT NULL,
    "% Nuls/Ins" DECIMAL NOT NULL,
    "% Nuls/Vot" DECIMAL NOT NULL,
    "Exprimés" DECIMAL NOT NULL,
    "% Exp/Ins" DECIMAL NOT NULL,
    "% Exp/Vot" DECIMAL NOT NULL,
    "N°Panneau" DECIMAL NOT NULL,
    "Sexe" VARCHAR(1) NOT NULL,
    "Nom" VARCHAR(13) NOT NULL,
    "Prénom" VARCHAR(8) NOT NULL,
    "Voix" DECIMAL NOT NULL,
    "% Voix/Ins" DECIMAL NOT NULL,
    "% Voix/Exp" DECIMAL NOT NULL,
    "Code Insee" DECIMAL NOT NULL,
    "Coordonnées" VARCHAR(255),
    "Nom Bureau Vote" VARCHAR(255),
    "Adresse" VARCHAR(255),
    "Code Postal" DECIMAL,
    "Ville" VARCHAR(255),
    uniq_bdv VARCHAR(255)
);

COPY elections FROM '/home/domainflag/Desktop/database/elections-loire.csv' DELIMITER ';' CSV HEADER ENCODING 'utf8';
COPY elections FROM '/home/domainflag/Desktop/database/elections-paris.csv' DELIMITER ';' CSV HEADER ENCODING 'utf8';

/* helper function to check a functional dependency */
CREATE OR REPLACE FUNCTION functional_dependency(c1 TEXT, c2 TEXT) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
    DECLARE
        is_fd INTEGER;
    BEGIN
        EXECUTE format('
            SELECT (
                SELECT COUNT(*) as val FROM (
                    SELECT DISTINCT %1$I FROM elections
                ) e1
            ) - (
                SELECT COUNT(*) as val FROM (
                    SELECT DISTINCT %1$I, %2$I FROM elections
                ) e2
            );
        ', c1, c2) INTO is_fd;

        RETURN is_fd;
    END;
$$;


/* retrieve every unique attribute for our schema */
CREATE OR REPLACE VIEW attribute AS (
    SELECT column_name AS col
    FROM information_schema.columns
    WHERE table_name = 'elections'
);


/* retrieve any possible functional dependency */
CREATE OR REPLACE VIEW dependency AS (
    SELECT a1.col AS c1, a2.col AS c2
    FROM attribute a1, attribute a2
    WHERE a1.col != a2.col AND functional_dependency(a1.col, a2.col) = 0
);


/* helper statement to create an empty cover for testing purposes  */
DROP TABLE IF EXISTS cover CASCADE;

CREATE TABLE cover (
    id SERIAL,
    attribute TEXT,
    cover TEXT ARRAY
);


/* helper procedure to fill all the fds singletons */
CREATE OR REPLACE PROCEDURE create_fds()
LANGUAGE plpgsql
AS $$
    DECLARE
        record dependency%rowtype;
    BEGIN
        FOR record IN
            SELECT * FROM dependency
        LOOP
            IF(record.c1 NOT IN (SELECT attribute FROM cover)) THEN
                INSERT INTO cover(attribute, cover) VALUES(record.c1::TEXT, ARRAY [record.c2::TEXT]);
            ELSE
                UPDATE cover SET cover = cover || record.c2::TEXT WHERE attribute = record.c1;
            END IF;
        END LOOP;
    END;
$$;


/* helper function for difference operation(between two text arrays) */
CREATE OR REPLACE FUNCTION array_diff(a TEXT ARRAY, b TEXT ARRAY) RETURNS TEXT ARRAY
LANGUAGE plpgsql
AS $$
    DECLARE
        r TEXT ARRAY;
    BEGIN
        SELECT array_agg(elements) INTO r
        FROM (
            SELECT unnest(a)
            EXCEPT
            SELECT unnest(b)
        ) t (elements);

        RETURN r;
    END;
$$;


/* helper procedure to compute X -> X+ */
CREATE OR REPLACE PROCEDURE create_cover()
LANGUAGE plpgsql
AS $$
    DECLARE
        record_outer cover%rowtype;
        record_inner cover%rowtype;

        attributes TEXT ARRAY;

        flag BOOLEAN DEFAULT TRUE;
        count INTEGER;
    BEGIN

        /* get number of records */
        SELECT COUNT(*) INTO count FROM cover;

        FOR g IN 1..count LOOP

            WHILE flag LOOP

                /* nothing to do more */
                flag = FALSE;

                /* get an outer record cover */
                SELECT * INTO record_outer FROM cover WHERE id = g;

                FOR h in 1..count LOOP

                    /* avoid redundant checks */
                    IF(g <> h) THEN

                        /* get an inner record cover */
                        SELECT * INTO record_inner FROM cover WHERE id = h;

                        /* inner fd attribute is a subset of outer fd cover  */
                        IF(record_inner.attribute = SOME(record_outer.cover)) THEN

                            /* find attributes that are non-existent in right cover */
                            SELECT array_diff(record_inner.cover, record_outer.cover) INTO attributes;

                            /* attribute array is not-empty */
                            IF(array_length(attributes, 1) != 0) THEN

                                /* there is more to do */
                                flag = TRUE;

                                /* update inner cover */
                                UPDATE cover SET cover = cover || attributes
                                WHERE id = g;

                            END IF;

                        END IF;

                    END IF;

                END LOOP;

            END LOOP;
        END LOOP;
    END;
$$;


/* helper function checking if a functional dependency can be resolved */
CREATE OR REPLACE FUNCTION check_dependency(attrib TEXT, attrib_cover TEXT ARRAY, verb BOOLEAN DEFAULT FALSE) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
    DECLARE
        record cover%rowtype;

        /* initialize flag for stacking series of updates */
        flag BOOLEAN DEFAULT TRUE;

        acc TEXT ARRAY;
        diff TEXT ARRAY;
    BEGIN

        /* initialize reflexivity - starting attribute */
        acc = ARRAY [ attrib ];

        WHILE flag LOOP

            /* if there is no record to be updated then exit loop */
            flag = FALSE;

            FOR record IN (SELECT * FROM cover) LOOP

                /* see if there is something to add */
                diff = array_diff(record.cover, acc);

                /* there is an occurrence (X -> X+) where X ∊ acc */
                IF(record.attribute = ANY(acc) AND array_length(diff, 1) != 0) THEN

                    /* verbose output for testing */
                    IF(verb) THEN
                        RAISE NOTICE '%', acc;
                    END IF;

                    /* adding matching attributes */
                    acc = array_cat(acc, diff);

                    /* if our acc contains every attribute within attrib_cover then the df can be resolved through armstrong relations */
                    IF(acc @> attrib_cover) THEN
                        RETURN TRUE;
                    END IF;

                    /* continue search */
                    flag = TRUE;

                END IF;

            END LOOP;

        END LOOP;

        RETURN FALSE;
    END;
$$;


/* helper procedure to calculate a minimal cover */
CREATE OR REPLACE PROCEDURE dependency_reduction()
LANGUAGE plpgsql
AS $$
    DECLARE
        record cover%rowtype;

        minimal_cover TEXT ARRAY;
        plain_cover TEXT ARRAY;

        attrib TEXT;
        count INTEGER;
    BEGIN

        /* count as result of select query is not mutable */
        SELECT COUNT(*) INTO count FROM cover;

        FOR g IN 1..count LOOP

            /* current record being looped */
            SELECT * INTO record FROM cover WHERE id = g;

            /* our cover to resolve given minimal attributes non-mutable */
            plain_cover = record.cover;

            /* initialize our minimal cover */
            minimal_cover = record.cover;

            FOREACH attrib IN ARRAY record.cover LOOP

                /* update with minimal cover */
                UPDATE cover SET cover = array_diff(minimal_cover, ARRAY [attrib]) WHERE id = record.id;

                IF(check_dependency(record.attribute, plain_cover)) THEN

                    /* updating our minimal cover */
                    minimal_cover = array_diff(minimal_cover, ARRAY [attrib]);

                ELSE

                    /* backtrack our minimal cover */
                    UPDATE cover SET cover = minimal_cover WHERE id = record.id;

                END IF;

            END LOOP;

        END LOOP;

    END;
$$;


/* helper procedure to remove records from table */
CREATE OR REPLACE PROCEDURE update_table(ids TEXT)
LANGUAGE plpgsql
AS $$
    DECLARE
        vars INTEGER ARRAY;
        record_id INTEGER;
    BEGIN

        /* store the record ids to be removed */
        SELECT string_to_array(ids, ', ') INTO vars;

        FOREACH record_id IN ARRAY vars LOOP

            /* remove record */
            DELETE FROM cover WHERE id = record_id;

        END LOOP;

        /* reset indices */
        ALTER SEQUENCE cover_id_seq RESTART WITH 1;
        UPDATE cover SET id = DEFAULT;

    END;
$$;


/* retrieve the data type for each column in original table */
CREATE OR REPLACE VIEW data_type AS (
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'elections'
);


/* helper function to normalize a database */
CREATE OR REPLACE PROCEDURE normalize_tables(verb BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
    DECLARE
        record cover%rowtype;

        table_key TEXT;

        count NUMERIC;

        exec_create_value TEXT;
        exec_insert_value TEXT;

        flag BOOLEAN;

        schemas cover ARRAY;
        schema cover;

        keys TEXT ARRAY;
        attribs TEXT ARRAY;
    BEGIN

        /* loop through covers that contain maximum information */
        FOR record IN (SELECT * FROM cover ORDER BY array_length(cover.cover, 1) DESC) LOOP

            /* initialize flag */
            flag = TRUE;

            IF(array_length(schemas, 1) > 0) THEN

                FOREACH schema IN ARRAY schemas LOOP

                    IF((record.cover || record.attribute) @> (schema.cover || schema.attribute)) THEN

                        /* redundant current outer schema */
                        flag = FALSE;

                        /* break out of loop */
                        EXIT;

                    END IF;

                END LOOP;

            END IF;

            IF(flag) THEN

                /* format both table name and the primary key for the corresponding table */
                SELECT quote_ident(lower(replace(record.attribute, ' ', '_'))) INTO table_key;

                /* create execution statements for data normalization */
                EXECUTE 'DROP TABLE IF EXISTS ' || table_key || ' CASCADE;';

                /* create table prepared statement */
                SELECT 'CREATE TABLE ' || table_key || ' (
                    ' || table_key || ' ' || (SELECT data_type FROM data_type WHERE column_name = record.attribute) ||  ' PRIMARY KEY, ' || (
                        SELECT string_agg(col_name || ' ' || col_type, ', ')
                        FROM  (
                            SELECT quote_ident(lower(replace(col_name, ' ', '_'))) AS col_name, (SELECT data_type FROM data_type WHERE column_name = col_name) AS col_type
                            FROM (
                                SELECT unnest(record.cover) AS col_name
                            ) b
                        ) a
                    ) || ');' INTO exec_create_value;

                /* insert table prepared statement */
                SELECT 'INSERT INTO ' || table_key || ' (' ||
                    'SELECT DISTINCT ' || (SELECT string_agg(quote_ident(value), ', ') FROM unnest(array_prepend(record.attribute, record.cover)) AS value) || ' ' ||
                    'FROM elections WHERE ' || quote_ident(record.attribute) || ' IS NOT NULL'
                || ');' INTO exec_insert_value;

                IF(verb) THEN

                    /* verbose output, for debugging dynamic prepared statements */
                    RAISE NOTICE E'%\n', exec_create_value;

                    RAISE NOTICE E'%\n\n', exec_insert_value;

                END IF;

                /* execute create statement */
                EXECUTE exec_create_value;

                /* execute insert statement */
                EXECUTE exec_insert_value;

                /* execute counter for inserted rows */
                EXECUTE 'SELECT COUNT(*) FROM ' || table_key || ';' INTO count;

                /* alert user about the number of inserted rows */
                RAISE NOTICE 'Inserted % rows into %', count, table_key;

                /* update current set of schemas */
                schemas = record || schemas;

            END IF;

        END LOOP;

        /* find unnecessary attributes to be discarded */
        FOREACH schema IN ARRAY schemas LOOP

            /* keys */
            keys = keys || schema.attribute;

            /* attributes */
            attribs = attribs || schema.cover;

        END LOOP;

        /* attributes to be within global normalized table */
        attribs = array_diff((
                SELECT array_agg(col::TEXT)
                FROM attribute
            ), array_diff(attribs, keys)
        );

        /* remove global table */
        EXECUTE 'DROP TABLE IF EXISTS elections_normalized CASCADE;';

        /* create global normalized table */
        SELECT 'CREATE TABLE elections_normalized (' || (
            SELECT string_agg(col_name || ' ' || col_type, ', ')
            FROM  (
                SELECT quote_ident(lower(replace(col_name, ' ', '_'))) AS col_name, (SELECT data_type FROM data_type WHERE column_name = col_name) AS col_type
                FROM (
                    SELECT unnest(attribs) AS col_name
                ) b
            ) a
        ) || ');' INTO exec_create_value;

        /* insert table prepared statement */
        SELECT 'INSERT INTO elections_normalized (' ||
            'SELECT DISTINCT ' || (SELECT string_agg(quote_ident(col_name), ', ') FROM unnest(attribs) AS col_name) || ' ' ||
            'FROM elections);' INTO exec_insert_value;

        IF(verb) THEN

            /* verbose output, for debugging dynamic prepared statements */
            RAISE NOTICE '%\n', exec_create_value;

            RAISE NOTICE '%\n\n', exec_insert_value;

        END IF;

        /* execute create statement */
        EXECUTE exec_create_value;

        /* execute insert statement */
        EXECUTE exec_insert_value;

        /* execute counter for inserted rows */
        SELECT COUNT(*) FROM elections_normalized INTO count;

        /* alert user about the number of inserted rows */
        RAISE NOTICE 'Inserted % rows into elections_normalized', count;

    END;
$$;



/* generate every df with left as singleton */
CALL create_fds();

\echo 'Initial set of fds:\n'

/* show the non-resolved cover */
SELECT * FROM cover;

/* prompt the user which fd is inaccurate */
\echo 'Enter ids of inaccurate fd(s) to be removed.\n'
\echo '(e.g) {} if none to be removed, or {1, 5, 2} to be removed rows with id 1, 5 and 2.'
\echo '{ and } to be removed! If using original data remove Prénom -> {Sexe} & Nom -> {N°Panneau,Sexe,Prénom} based on their ids'

\prompt 'Enter: ' values

/* removing inaccurate fds */
CALL update_table(:'values');

/* compute X -> X+ */
CALL create_cover();

/* performing right reduction */
CALL dependency_reduction();

/* print current fds */
SELECT * FROM cover;

/* performing normalization */
CALL normalize_tables();

/* enter a candidate */
\prompt 'Enter a candidate (e.g Macron): ' candidate

/* show vote percentage */
\echo 'Global percentage candidate ' :'candidate'

/* show vote and percentage global for chosen candidate */
SELECT (
    round(
        (SELECT SUM(Voix) FROM elections_normalized NATURAL JOIN "n°panneau" WHERE lower(nom) LIKE lower(:'candidate'))
        /
        (SELECT SUM(Voix) FROM elections_normalized)
        * 100
    , 2)
) AS "Votes %";


/* show vote and percentage per département and name */
\echo 'Votes and candidate percentage to respect of département'

SELECT "département" AS depart, "nom" AS name, SUM(Voix) AS votes, round(SUM(Voix) * 100.0 / SUM(SUM(Voix)) OVER (PARTITION BY "département"), 2) AS percentage
FROM elections_normalized NATURAL JOIN "code_du_département" NATURAL JOIN "n°panneau"
GROUP BY "département", "nom" ORDER BY "département", "nom" ASC LIMIT 30;


/* show vote and percentage per circonscription and name */
\echo 'Votes and candidate percentage to respect of circonscription'

SELECT "circonscription" AS circon, "nom" AS name, SUM(Voix) AS votes, round(SUM(Voix) * 100.0 / SUM(SUM(Voix)) OVER (PARTITION BY "circonscription"), 2) AS percentage
FROM elections_normalized NATURAL JOIN "circonscription" NATURAL JOIN "n°panneau"
GROUP BY "circonscription", "nom" ORDER BY "circonscription", "nom" ASC LIMIT 30;


/* show vote and percentage per commune and name */
\echo 'Votes and candidate percentage to respect of commune'

SELECT "commune", "nom" AS name, SUM(Voix) AS votes, round(SUM(Voix) * 100.0 / SUM(SUM(Voix)) OVER (PARTITION BY "commune"), 2) AS percentage
FROM elections_normalized NATURAL JOIN "commune" NATURAL JOIN "n°panneau"
GROUP BY "commune", "nom" ORDER BY "commune", "nom" ASC LIMIT 30;


/* show vote and percentage per Nom Bureau Vote and name */
\echo 'Votes and candidate percentage to respect of Nom Bureau Vote'

SELECT "nom_bureau_vote", "nom" AS name, SUM(Voix) AS votes, round(SUM(Voix) * 100.0 / SUM(SUM(Voix)) OVER (PARTITION BY "nom_bureau_vote"), 2) AS percentage
FROM elections_normalized NATURAL JOIN "uniq_bdv" NATURAL JOIN "n°panneau"
GROUP BY "nom_bureau_vote", "nom" ORDER BY "nom_bureau_vote", "nom" ASC LIMIT 30;
