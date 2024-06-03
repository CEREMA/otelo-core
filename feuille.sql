## get_table_epci::df
SELECT * FROM {0} ORDER BY "EPCI"{1};

## get_table_zo::df
SELECT * FROM {0} ORDER BY "ZO"{1};

## get_epcis::list
SELECT DISTINCT "EPCI" FROM {0};

## get_zos::list
SELECT DISTINCT "ZO" FROM {0};

## get_val_epci::smart
SELECT "{0}" FROM {1} WHERE "EPCI"='{2}';

## get_val_zo::smart
SELECT "{0}" FROM {1} WHERE "ZO"='{2}';

## get_expr_epci::smart
SELECT {0} FROM {1} WHERE "EPCI"='{2}';

## get_expr_zo::smart
SELECT {0} FROM {1} WHERE "ZO"='{2}';

## get_val_omphale_epci::smart
SELECT "{0}" FROM {1} WHERE "EPCI"='{2}' AND annee={3};

## get_val_omphale_zo::smart
SELECT "{0}" FROM {1} WHERE "ZO"='{2}' AND annee={3};
