#!/usr/bin/env python3

# Test the values tuple structure
values = (
    'importer_name', 'rfc', 'total_pedimentos_last_6_months', 
    'total_freight_usd_value', 'avg_freight_usd_per_shipment', 
    'customs_offices_used', 'pct_shipments_key_locations',
    'pct_regime_A1', 'pct_regime_F4', 'pct_regime_IN', 
    'pct_regime_A3', 'pct_regime_AF',
    'pct_transport_carretero', 'pct_transport_aereo', 
    'pct_transport_maritimo', 'pct_transport_not_declared',
    'pct_port_NUEVO_LAREDO', 'pct_port_COLOMBIA_NL', 
    'pct_port_MONTERREY_AIRPORT', 'pct_port_MANZANILLO',
    'pct_port_PUEBLA', 'pct_port_OTHERS', 'pct_hs_84', 
    'pct_hs_85', 'pct_hs_90', 'pct_hs_73', 'pct_hs_74',
    'pct_hs_OTROS', 'is_origin_usa', 'is_candidate_for_crossborder', 
    'pct_incoterm_DAP', 'pct_incoterm_EXW',
    'pct_incoterm_FCA', 'pct_incoterm_OTROS', 'custom_brokers_used', 
    'top_custom_broker_id', 'pct_top_custom_broker_id', 
    'num_custom_brokers_used', 'pct_broker_3995', 'pct_broker_3714', 'pct_broker_1720', 'pct_origin_TAIWAN', 
    'pct_origin_VIETNAM', 'pct_origin_CHINA', 'pct_origin_USA',
    'pct_origin_GERMANY', 'pct_origin_DENMARK', 'pct_origin_FRANCE', 
    'pct_origin_OTROS', 'last_import_date', 'first_import_date', 
    'total_weight_kg', 'avg_weight_per_shipment',
    'business_opportunity_score', 'crossborder_potential', 
    'ocean_freight_potential', 'supply_chain_potential'
)

print(f"Number of values: {len(values)}")
print("Values:")
for i, val in enumerate(values):
    print(f"{i+1}: {val}")

