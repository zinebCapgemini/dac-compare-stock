
to launch this script 3 steps

1 - npm i 

2 - add database configuration to getStock.js ( config_flat & config_atomic)

3 - node --max-old-space-size=8000 getStock.js


atomic.csv : les sku dans l'atomic  <br/>
flat.csv : les sku dans la falt  <br/>
diff.csv : les sku qui existe à la fois dans l'atomic et la flat et ils sont différents  <br/>
diff_not_exist_flat.csv :  les sku qui existe dans l'atomic, mais jamais descendu dans la flat  <br/>
diff_retail.csv : Différence de stock retail entre sku_country et product dans la flat <br/>
diff_ecom_format.csv : Différence de stock ecom les sku sont formatés pour être utilisés dans une requête avec IN <br/>
diff_retail_format.csv : Différence de stock retail les sku sont formatés pour être utilisés dans une requête avec IN <br/>