ADDRESS_TRANSACTIONS_QUERY = """
MATCH (a:Address {address: $address})
OPTIONAL MATCH (a)-[:INPUT|OUTPUT]->(t:Transaction)
RETURN a, t
"""

CREATE_OR_UPDATE_BTC_TRANSACTION_BATCH = """
UNWIND $rows AS row
MERGE (t:Transaction {hash: row.hash})
ON CREATE SET
  t.block_id = row.block_id,
  t.time = row.time,
  t.size = row.size,
  t.weight = row.weight,
  t.version = row.version,
  t.lock_time = row.lock_time,
  t.is_coinbase = row.is_coinbase,
  t.has_witness = row.has_witness,
  t.input_count = row.input_count,
  t.output_count = row.output_count,
  t.input_total = row.input_total,
  t.input_total_usd = row.input_total_usd,
  t.output_total = row.output_total,
  t.output_total_usd = row.output_total_usd,
  t.fee = row.fee,
  t.fee_usd = row.fee_usd,
  t.fee_per_kb = row.fee_per_kb,
  t.fee_per_kb_usd = row.fee_per_kb_usd,
  t.fee_per_kwu = row.fee_per_kwu,
  t.fee_per_kwu_usd = row.fee_per_kwu_usd,
  t.cdd_total = row.cdd_total
"""

CREATE_INPUT_BATCH = """
UNWIND $rows AS row
MERGE (t:Transaction {hash: row.transaction_hash})
MERGE (a:Address {address: row.recipient})
CREATE (a)-[:INPUT {
  index: row.index,
  time: row.time,
  value: row.value,
  value_usd: row.value_usd,
  type: row.type,
  script_hex: row.script_hex,
  is_from_coinbase: row.is_from_coinbase,
  is_spendable: row.is_spendable,
  spending_block_id: row.spending_block_id,
  spending_transaction_hash: row.spending_transaction_hash,
  spending_index: row.spending_index,
  spending_time: row.spending_time,
  spending_value_usd: row.spending_value_usd,
  spending_sequence: row.spending_sequence,
  spending_signature_hex: row.spending_signature_hex,
  spending_witness: row.spending_witness,
  lifespan: row.lifespan,
  cdd: row.cdd
}]->(t)
"""

CREATE_OUTPUT_BATCH = """
UNWIND $rows AS row
MERGE (t:Transaction {hash: row.transaction_hash})
MERGE (a:Address {address: row.recipient})
CREATE (t)-[:OUTPUT {
  index: row.index,
  time: row.time,
  value: row.value,
  value_usd: row.value_usd,
  type: row.type,
  script_hex: row.script_hex,
  is_from_coinbase: row.is_from_coinbase,
  is_spendable: row.is_spendable
}]->(a)
"""

transaction_defaults = {
    'block_id': None, 'time': None, 'size': None, 'weight': None, 'version': None, 'lock_time': None,
    'is_coinbase': False, 'has_witness': False, 'input_count': 0, 'output_count': 0, 'input_total': 0,
    'input_total_usd': 0.0, 'output_total': 0, 'output_total_usd': 0.0, 'fee': 0, 'fee_usd': 0.0,
    'fee_per_kb': 0.0, 'fee_per_kb_usd': 0.0, 'fee_per_kwu': 0.0, 'fee_per_kwu_usd': 0.0, 'cdd_total': 0.0
}

input_defaults = {
    'block_id': None, 'index': None, 'time': None, 'value': 0, 'value_usd': 0.0, 'recipient': None, 'type': None,
    'script_hex': None, 'is_from_coinbase': False, 'is_spendable': True, 'spending_block_id': None,
    'spending_transaction_hash': None, 'spending_index': None, 'spending_time': None, 'spending_value_usd': 0.0,
    'spending_sequence': None, 'spending_signature_hex': None, 'spending_witness': None, 'lifespan': 0.0, 'cdd': 0.0
}

output_defaults = {
    'block_id': None, 'index': None, 'time': None, 'value': 0, 'value_usd': 0.0, 'recipient': None, 'type': None,
    'script_hex': None, 'is_from_coinbase': False, 'is_spendable': True
}

def fill_defaults(row_dict, defaults):
    for key, value in defaults.items():
        if key not in row_dict:
            row_dict[key] = value
    return row_dict

def create_or_update_btc_transaction(tx, params):
    params = [fill_defaults(row, transaction_defaults) for row in params]
    tx.run(CREATE_OR_UPDATE_BTC_TRANSACTION_BATCH, rows=params)

def create_input(tx, params):
    params = [fill_defaults(row, input_defaults) for row in params]
    tx.run(CREATE_INPUT_BATCH, rows=params)

def create_output(tx, params):
    params = [fill_defaults(row, output_defaults) for row in params]
    tx.run(CREATE_OUTPUT_BATCH, rows=params)
