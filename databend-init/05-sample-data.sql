-- Sample data for testing query pools
-- This is minimal test data, not realistic benchmark data
-- Uses fully qualified table names to avoid session state issues

-- ============ TPC-H Sample Data ============

-- Region (5 rows - standard TPC-H)
INSERT INTO tpch1g.region VALUES
(0, 'AFRICA', 'special requests'),
(1, 'AMERICA', 'special instructions'),
(2, 'ASIA', 'express deposits'),
(3, 'EUROPE', 'furiously regular'),
(4, 'MIDDLE EAST', 'carefully final');

-- Nation (25 rows - standard TPC-H)
INSERT INTO tpch1g.nation VALUES
(0, 'ALGERIA', 0, 'haggle. carefully final'),
(1, 'ARGENTINA', 1, 'interleave about the slyly'),
(2, 'BRAZIL', 1, 'requests haggle carefully'),
(3, 'CANADA', 1, 'eas hang ironic, silent'),
(4, 'EGYPT', 4, 'y above the carefully'),
(5, 'ETHIOPIA', 0, 'ven packages wake'),
(6, 'FRANCE', 3, 'refully final requests'),
(7, 'GERMANY', 3, 'l platelets. regular accounts'),
(8, 'INDIA', 2, 'ss excuses cajole slyly'),
(9, 'INDONESIA', 2, 'slyly express asymptotes'),
(10, 'IRAN', 4, 'efully alongside of the slyly'),
(11, 'IRAQ', 4, 'nic deposits boost'),
(12, 'JAPAN', 2, 'ously. final, express gifts'),
(13, 'JORDAN', 4, 'ic deposits are blithely'),
(14, 'KENYA', 0, 'pending excuses haggle'),
(15, 'MOROCCO', 0, 'rns. blithely bold courts'),
(16, 'MOZAMBIQUE', 0, 's. ironic, unusual asymptotes'),
(17, 'PERU', 1, 'platelets. blithely pending'),
(18, 'CHINA', 2, 'c dependencies. furiously'),
(19, 'ROMANIA', 3, 'ular asymptotes are about'),
(20, 'SAUDI ARABIA', 4, 'ts. silent requests haggle'),
(21, 'VIETNAM', 2, 'hely enticingly express'),
(22, 'RUSSIA', 3, 'requests against the platelets'),
(23, 'UNITED KINGDOM', 3, 'eans boost carefully special'),
(24, 'UNITED STATES', 1, 'y final packages. slow');

-- Supplier (sample)
INSERT INTO tpch1g.supplier VALUES
(1, 'Supplier#000000001', 'N kD4on9OM Ipw3,gf0JBoQDd7tgr', 17, '27-918-335-1736', 5755.94, 'each slyly above the careful'),
(2, 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', 5, '15-679-861-2259', 4032.68, 'furiously stealthy frays'),
(3, 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', 1, '11-383-516-1199', 4192.40, 'furiously regular instructions');

-- Part (sample)
INSERT INTO tpch1g.part VALUES
(1, 'goldenrod lavender spring chocolate', 'Manufacturer#1', 'Brand#13', 'PROMO BURNISHED COPPER', 7, 'JUMBO PKG', 901.00, 'final deposits'),
(2, 'blush thistle blue yellow', 'Manufacturer#1', 'Brand#13', 'LARGE BRUSHED BRASS', 1, 'LG CASE', 902.00, 'slyly ironic deposits'),
(3, 'spring green yellow purple', 'Manufacturer#4', 'Brand#42', 'STANDARD POLISHED BRASS', 21, 'WRAP CASE', 903.00, 'special ideas promise');

-- PartSupp (sample)
INSERT INTO tpch1g.partsupp VALUES
(1, 1, 3325, 771.64, 'requests cajole slyly'),
(1, 2, 8076, 993.49, 'bold requests across the unusual'),
(2, 1, 4651, 337.09, 'blithely final deposits');

-- Customer (sample)
INSERT INTO tpch1g.customer VALUES
(1, 'Customer#000000001', 'IVhzIApeRb', 15, '25-989-741-2988', 711.56, 'BUILDING', 'regular, pending accounts'),
(2, 'Customer#000000002', 'XSTf4,NCwDVaWNe6tEgv', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'furiously express'),
(3, 'Customer#000000003', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', 'special theodolites haggle');

-- Orders (sample)
INSERT INTO tpch1g.orders VALUES
(1, 1, 'O', 173665.47, '1996-01-02', '5-LOW', 'Clerk#000000951', 0, 'blithely final dolphins'),
(2, 2, 'O', 46929.18, '1996-12-01', '1-URGENT', 'Clerk#000000880', 0, 'furiously unusual packages'),
(3, 3, 'F', 193846.25, '1993-10-14', '5-LOW', 'Clerk#000000955', 0, 'slyly even packages');

-- Lineitem (sample)
INSERT INTO tpch1g.lineitem VALUES
(1, 1, 1, 1, 17.00, 21168.23, 0.04, 0.02, 'N', 'O', '1996-03-13', '1996-02-12', '1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'regular courts'),
(1, 2, 1, 2, 36.00, 34850.16, 0.09, 0.06, 'N', 'O', '1996-04-12', '1996-02-28', '1996-04-20', 'TAKE BACK RETURN', 'MAIL', 'special deposits'),
(2, 3, 2, 1, 38.00, 44694.46, 0.00, 0.05, 'N', 'O', '1997-01-28', '1997-01-14', '1997-02-02', 'TAKE BACK RETURN', 'RAIL', 'special requests');

-- ============ TPC-DS Sample Data ============

-- date_dim (sample)
INSERT INTO tpcds1g.date_dim VALUES
(2415022, 'AAAAAAAACAAAAAAA', '1900-01-02', 1, 1, 1, 1900, 2, 1, 2, 1, 1900, 1, 1, 'Monday', '1900Q1', 'N', 'N', 'N', 2415021, 2415051, NULL, NULL, 'N', 'N', 'N', 'N', 'N'),
(2450816, 'AAAAAAAAAMGAAAAA', '1998-01-02', 1, 1, 1, 1998, 5, 1, 2, 1, 1998, 1, 1, 'Friday', '1998Q1', 'N', 'N', 'N', 2450815, 2450845, 2450451, 2450724, 'N', 'N', 'Y', 'N', 'N'),
(2451180, 'AAAAAAAAOAGBAAAA', '1999-01-02', 1, 1, 1, 1999, 6, 1, 2, 1, 1999, 1, 1, 'Saturday', '1999Q1', 'N', 'Y', 'N', 2451179, 2451209, 2450815, 2451088, 'N', 'N', 'N', 'Y', 'N'),
(2451546, 'AAAAAAAACKHBAAAA', '2000-01-03', 1, 2, 1, 2000, 1, 1, 3, 1, 1999, 53, 1, 'Monday', '2000Q1', 'N', 'N', 'N', 2451545, 2451576, 2451181, 2451454, 'N', 'N', 'N', 'N', 'Y'),
(2451911, 'AAAAAAAAPOHBAAAA', '2001-01-02', 1, 1, 1, 2001, 2, 1, 2, 1, 2001, 1, 1, 'Tuesday', '2001Q1', 'N', 'N', 'N', 2451910, 2451940, 2451546, 2451819, 'N', 'Y', 'N', 'N', 'N');

-- store (sample)
INSERT INTO tpcds1g.store VALUES
(1, 'AAAAAAAABAAAAAAA', '1997-03-13', NULL, NULL, 'ought', 200, 5250, '8AM-12AM', 'William Ward', 6, 'Unknown', 'Rooms must make permanently', 'Michael White', 1, 'Unknown', 1, 'Unknown', '651', 'Sixth', 'ST', 'Suite 180', 'Midway', 'Williamson County', 'TN', '31904', 'United States', -5.00, 0.11);

-- customer (sample)
INSERT INTO tpcds1g.customer VALUES
(1, 'AAAAAAAABAAAAAAA', 980124, 7135, 32946, 2452238, 2452208, 'Mr.', 'Javier', 'Lewis', 'Y', 9, 12, 1936, 'CHILE', NULL, 'Javier.Lewis@VFAxlnZEvOx.org', '2452508');

-- item (sample)
INSERT INTO tpcds1g.item VALUES
(1, 'AAAAAAAABAAAAAAA', '1997-10-27', NULL, 'ought', 1.09, 0.58, 1, 'importoamalg #1', 1, 'athletic shoes', 1, 'Women', 1, 'oughtought', 'small', NULL, 'forest', 'Unknown', 'Unknown', 1, 'able');

-- store_returns (sample)
INSERT INTO tpcds1g.store_returns VALUES
(2451911, 30756, 1, 1, 31, 32, 22574, 1, 1, 2, 8, 15.25, 1.06, 16.31, 0.57, 5.46, 0.00, 7.60, 6.12, 2.06);

-- ============ IMDB Sample Data ============

-- kind_type
INSERT INTO imdb.kind_type VALUES
(1, 'movie'),
(2, 'tv series'),
(3, 'tv movie'),
(4, 'video movie'),
(5, 'tv mini series'),
(6, 'video game'),
(7, 'episode');

-- role_type
INSERT INTO imdb.role_type VALUES
(1, 'actor'),
(2, 'actress'),
(3, 'producer'),
(4, 'writer'),
(5, 'cinematographer'),
(6, 'composer'),
(7, 'costume designer'),
(8, 'director'),
(9, 'editor'),
(10, 'miscellaneous crew'),
(11, 'production designer'),
(12, 'guest');

-- company_type
INSERT INTO imdb.company_type VALUES
(1, 'distributors'),
(2, 'production companies');

-- info_type
INSERT INTO imdb.info_type VALUES
(1, 'runtimes'),
(2, 'color info'),
(3, 'genres'),
(4, 'languages'),
(5, 'certificates'),
(6, 'sound mix'),
(7, 'tech info'),
(8, 'countries'),
(9, 'taglines'),
(10, 'keywords');

-- title (sample movies)
INSERT INTO imdb.title VALUES
(1, 'The Matrix', NULL, 1, 1999, NULL, 'M362', NULL, NULL, NULL, NULL, NULL),
(2, 'Inception', NULL, 1, 2010, NULL, 'I523', NULL, NULL, NULL, NULL, NULL),
(3, 'Pulp Fiction', NULL, 1, 1994, NULL, 'P412', NULL, NULL, NULL, NULL, NULL);

-- name (sample actors)
INSERT INTO imdb.name VALUES
(1, 'Reeves, Keanu', NULL, NULL, 'm', 'R12', 'K5', 'R12', NULL),
(2, 'DiCaprio, Leonardo', NULL, NULL, 'm', 'D216', 'L5636', 'D216', NULL),
(3, 'Travolta, John', NULL, NULL, 'm', 'T614', 'J5', 'T614', NULL);

-- char_name (sample characters)
INSERT INTO imdb.char_name VALUES
(1, 'Neo', NULL, NULL, 'N', 'N', NULL),
(2, 'Dom Cobb', NULL, NULL, 'D512', 'D512', NULL),
(3, 'Vincent Vega', NULL, NULL, 'V5253', 'V5253', NULL);

-- company_name (sample)
INSERT INTO imdb.company_name VALUES
(1, 'Warner Bros.', '[us]', NULL, 'W656', 'W656', NULL),
(2, 'Universal Pictures', '[us]', NULL, 'U5162', 'U5162', NULL),
(3, 'Miramax', '[us]', NULL, 'M62', 'M62', NULL),
(4, 'Mosfilm', '[ru]', NULL, 'M214', 'M214', NULL);

-- cast_info (sample)
INSERT INTO imdb.cast_info VALUES
(1, 1, 1, 1, '(voice) (uncredited)', 1, 1),
(2, 2, 2, 2, NULL, 1, 1),
(3, 3, 3, 3, NULL, 1, 1);

-- movie_companies (sample)
INSERT INTO imdb.movie_companies VALUES
(1, 1, 1, 2, NULL),
(2, 2, 2, 2, NULL),
(3, 3, 3, 2, NULL),
(4, 1, 4, 2, NULL);
