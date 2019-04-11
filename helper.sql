/*********************************************************************/
/* helper function to create an empty cover for testing purposes  */
CREATE OR REPLACE PROCEDURE create_cover()
LANGUAGE plpgsql
AS $$
    BEGIN
        EXECUTE FORMAT('
            DROP TABLE IF EXISTS cover;
            CREATE TABLE cover (
                id SERIAL,
                attribute VARCHAR(255),
                cover VARCHAR(255) ARRAY
            )
        ');
    END;
$$;

/*********************************************************************/
/* helper function for difference operation */
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

/* ['ab', 'bc'] / ['bc', 'de', 'a'] = ['ab'] */
SELECT array_diff(ARRAY ['ab', 'bc'], ARRAY ['bc', 'de', 'a']);

/* ['ab', 'bc', 'de'] / ['de'] = ['ab', 'bc'] */
SELECT array_diff(ARRAY ['ab', 'bc', 'de'], ARRAY ['de']);



/*********************************************************************/
BEGIN;

CALL create_cover();

INSERT INTO cover(attribute, cover) VALUES('B', ARRAY ['AD']);
INSERT INTO cover(attribute, cover) VALUES('ABE', ARRAY ['D']);
INSERT INTO cover(attribute, cover) VALUES('DE', ARRAY ['B']);
INSERT INTO cover(attribute, cover) VALUES('AD', ARRAY ['BCE']);
INSERT INTO cover(attribute, cover) VALUES('BC', ARRAY ['A']);

INSERT INTO cover(attribute, cover) VALUES('D', ARRAY ['F']);
INSERT INTO cover(attribute, cover) VALUES('B', ARRAY ['D', 'E']);
INSERT INTO cover(attribute, cover) VALUES('A', ARRAY ['B', 'C']);

CREATE OR REPLACE PROCEDURE self_dependency_cover()
LANGUAGE plpgsql
AS $$
    DECLARE
        record_outer cover%rowtype;
        record_inner cover%rowtype;
        attribute TEXT;
        attribute_unique TEXT;
        flag BOOLEAN;
        flag_subset BOOLEAN;
        count INTEGER;
    BEGIN
        SELECT COUNT(*) INTO count FROM cover;

        FOR g IN 1..count LOOP
            flag = TRUE;

            WHILE flag LOOP
                SELECT * INTO record_outer FROM cover WHERE id = g;

                flag = FALSE;

                FOR h in 1..count LOOP

                    SELECT * INTO record_inner FROM cover WHERE id = h;

                    SELECT record_inner.attribute = SOME(record_outer.cover) INTO flag_subset;

                    IF(flag_subset) THEN
                        FOREACH attribute_unique IN ARRAY record_inner.cover LOOP
                            IF(NOT attribute_unique = SOME(record_outer.cover)) THEN
                                -- Need to go further
                                flag = TRUE;

                                -- Update the full cover
                                UPDATE cover
                                SET cover = array_append(cover, attribute_unique)
                                WHERE id = g;

                            END IF;
                        END LOOP;

                    END IF;

                END LOOP;

            END LOOP;
        END LOOP;
    END;
$$;

CALL self_dependency_cover();

ROLLBACK;


/*********************************************************************/
BEGIN;

CALL create_cover();

INSERT INTO cover(attribute, cover) VALUES('A', ARRAY ['F']);
INSERT INTO cover(attribute, cover) VALUES('D', ARRAY ['A', 'N']);
INSERT INTO cover(attribute, cover) VALUES('N', ARRAY ['E', 'C']);

CREATE OR REPLACE FUNCTION check_dependency(attrib TEXT, attrib_cover TEXT ARRAY, verb BOOLEAN DEFAULT FALSE) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
    DECLARE
        record cover%rowtype;

        flag BOOLEAN;
        acc TEXT ARRAY;
        diff TEXT ARRAY;
    BEGIN

        /* initialize reflexivity */
        acc = ARRAY [ attrib ];

        /* initialize flag for stacking update */
        flag = TRUE;

        WHILE flag LOOP

            /* if there is no update then exit loop */
            flag = FALSE;

            FOR record IN (SELECT * FROM cover) LOOP

                /* see if there is something to add */
                diff = array_diff(record.cover, acc);

                /* there is an occurrence (X -> X+) where X ∊ acc */
                IF(record.attribute = ANY(acc) AND NOT array_length(diff, 1) = 0) THEN

                    IF(verb) THEN
                        RAISE NOTICE '%', acc;
                    END IF;

                    /* adding matching attributes */
                    acc = array_cat(acc, diff);

                    RAISE NOTICE '%', acc;

                    /* if our acc contains every attribute within attrib_cover then resolved */
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

/* true */
SELECT check_dependency('D', ARRAY ['A', 'E', 'F'], TRUE);
/* false as there is no b */
SELECT check_dependency('D', ARRAY ['A', 'B', 'F'], TRUE);

ROLLBACK;


/* helper function to fill all the fds left part */
CREATE OR REPLACE PROCEDURE create_left_dfs()
LANGUAGE plpgsql
AS $$
    DECLARE
        record1 cover%rowtype;
        record2 cover%rowtype;

        index INTEGER;
        count INTEGER;
    BEGIN
        /* count to loop on range */
        SELECT COUNT(*) INTO count FROM cover;

        FOR g IN 1..count LOOP

            /* checking if the record is pertinent */
            SELECT * INTO record1 FROM cover WHERE id = g;

            /* search for one row that composition can be made on */
            SELECT * INTO record2 FROM cover WHERE id > g AND cover @> record1.cover AND record1.cover @> cover LIMIT 1;

            IF(record2 IS NOT NULL) THEN

                /* update the attributes and compose two dfs */
                UPDATE cover SET attributes = attributes || array_diff(record1.attributes, attributes) WHERE id = record2.id;

                /* remove unnecessary df */
                DELETE FROM cover WHERE id = g;

            END IF;

        END LOOP;
    END;
$$;
