const masterForm = document.getElementById('master-form');
const masterName = document.getElementById('master-name');
const masterFields = document.getElementById('master-fields');
const masterResult = document.getElementById('master-result');
const masterTableHeadRow = document.getElementById('master-table-head-row');
const masterTableBody = document.getElementById('master-table-body');
const masterFormTitle = document.getElementById('master-form-title');
const masterRecordId = document.getElementById('master-record-id');
const saveMasterBtn = document.getElementById('save-master-btn');
const cancelMasterEditBtn = document.getElementById('cancel-master-edit-btn');

const oppForm = document.getElementById('opp-form');
const oppResult = document.getElementById('opp-result');
const allPropsBody = document.getElementById('all-props-body');
const historyPanel = document.getElementById('history-panel');
const historyTitle = document.getElementById('history-title');
const historyBody = document.getElementById('history-body');
const closeHistoryBtn = document.getElementById('close-history-btn');
const decisaoPanel = document.getElementById('decisao-panel');
const decisaoTitle = document.getElementById('decisao-title');
const decisaoForm = document.getElementById('decisao-form');
const decisaoResult = document.getElementById('decisao-result');
const closeDecisaoBtn = document.getElementById('close-decisao-btn');
const cancelDecisaoBtn = document.getElementById('cancel-decisao-btn');
const decisaoOportunidadeId = document.getElementById('decisao-oportunidade-id');
const decisaoUpdateSelect = document.getElementById('decisao-update-id');
const justificativaUpdateSelect = document.getElementById('justificativa-update-id');
const oppFormTitle = document.getElementById('opp-form-title');
const saveOppBtn = document.getElementById('save-opp-btn');
const cancelEditBtn = document.getElementById('cancel-edit-btn');
const oportunidadeIdInput = document.getElementById('oportunidade_id');

let decisoesCache = [];
let currentAllOppRows = [];

let meta = {};
let currentMasterRows = [];

function formatVersao(value) {
  if (value == null) {
    return '';
  }
  const text = String(value).trim();
  if (!text) {
    return '';
  }
  if (/^v\d+$/i.test(text)) {
    return `V${Number.parseInt(text.slice(1), 10)}`;
  }
  const numeric = Number.parseInt(text, 10);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return '';
  }
  return `V${numeric}`;
}

function normalizeHistoryValue(value) {
  if (value == null) return '';
  return String(value).trim();
}

function changedHistoryClass(currentRow, previousRow, field) {
  if (!previousRow) return '';
  const current = normalizeHistoryValue(currentRow[field]);
  const previous = normalizeHistoryValue(previousRow[field]);
  return current !== previous ? ' class="history-changed"' : '';
}

const fkMapping = {
  cliente_id: { cadastro: 'clientes', labelField: 'nome' },
  segmento_id: { cadastro: 'segmentos', labelField: 'nome' },
  canal_id: { cadastro: 'canais', labelField: 'nome' },
  responsavel_1_id: { cadastro: 'resposaveis_pessoaTema', labelField: 'nome' },
  responsavel_2_id: { cadastro: 'resposaveis_pessoaTema', labelField: 'nome' },
  categoria_id: { cadastro: 'categorias', labelField: 'nome' },
  area_negocio_id: { cadastro: 'areas_negocio', labelField: 'nome' },
  estado_id: { cadastro: 'estados', labelField: 'uf' },
  decisao_id: { cadastro: 'decisoes', labelField: 'nome' },
  justificativa_id: { cadastro: 'justificativas', labelField: 'nome' },
  status_proposta_id: { cadastro: 'status_proposta', labelField: 'nome' },
};

function labelize(name) {
  return name.replaceAll('_', ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

function isNoGoLabel(text) {
  const normalized = (text || '').toUpperCase();
  return normalized.includes('NO-GO') || normalized.includes('NO GO') || normalized.includes('NOGO');
}

function updateJustificativaRuleFromDecisionPanel() {
  if (!decisaoUpdateSelect || !justificativaUpdateSelect) return;

  const selectedOption = decisaoUpdateSelect.options[decisaoUpdateSelect.selectedIndex];
  const label = selectedOption ? selectedOption.textContent : '';
  const isNoGo = isNoGoLabel(label);

  justificativaUpdateSelect.disabled = !isNoGo;
  justificativaUpdateSelect.required = isNoGo;
  if (!isNoGo) {
    justificativaUpdateSelect.value = '';
  }
}

function closeDecisaoPanel() {
  decisaoForm.reset();
  decisaoResult.textContent = '';
  decisaoPanel.style.display = 'none';
}

function openDecisaoPanel(row) {
  decisaoOportunidadeId.value = row.oportunidade_id;
  decisaoTitle.textContent = `Alterar Estado da Decisao - ${row.codigo_proposta ?? `#${row.oportunidade_id}`}`;
  decisaoUpdateSelect.value = row.decisao_id == null ? '' : String(row.decisao_id);
  justificativaUpdateSelect.value = row.justificativa_id == null ? '' : String(row.justificativa_id);
  updateJustificativaRuleFromDecisionPanel();
  decisaoPanel.style.display = 'block';
  decisaoPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function loadDecisionPanelOptions() {
  if (!decisaoUpdateSelect || !justificativaUpdateSelect) return;

  const [decisoesResponse, justificativasResponse] = await Promise.all([
    fetch('/api/cadastros/decisoes'),
    fetch('/api/cadastros/justificativas'),
  ]);
  const decisoes = await decisoesResponse.json();
  const justificativas = await justificativasResponse.json();
  decisoesCache = decisoes;

  decisaoUpdateSelect.innerHTML = '';
  const emptyDecisao = document.createElement('option');
  emptyDecisao.value = '';
  emptyDecisao.textContent = 'Selecione';
  decisaoUpdateSelect.appendChild(emptyDecisao);
  for (const row of decisoes) {
    const opt = document.createElement('option');
    opt.value = row.id;
    opt.textContent = `${row.id} - ${row.nome ?? ''}`;
    decisaoUpdateSelect.appendChild(opt);
  }

  justificativaUpdateSelect.innerHTML = '';
  const emptyJust = document.createElement('option');
  emptyJust.value = '';
  emptyJust.textContent = 'Selecione';
  justificativaUpdateSelect.appendChild(emptyJust);
  for (const row of justificativas) {
    const opt = document.createElement('option');
    opt.value = row.id;
    opt.textContent = `${row.id} - ${row.nome ?? ''}`;
    justificativaUpdateSelect.appendChild(opt);
  }

  updateJustificativaRuleFromDecisionPanel();
}

async function loadMeta() {
  const response = await fetch('/api/meta/cadastros');
  meta = await response.json();

  masterName.innerHTML = '';
  for (const cadastro of Object.keys(meta)) {
    const cfg = meta[cadastro];
    const opt = document.createElement('option');
    opt.value = cadastro;
    opt.textContent = cfg.display_name || labelize(cadastro);
    masterName.appendChild(opt);
  }

  renderMasterFields();
}

function renderMasterFields() {
  const cadastro = masterName.value;
  const cfg = meta[cadastro];
  masterFields.innerHTML = '';

  for (const field of cfg.fields) {
    const label = document.createElement('label');
    label.textContent = labelize(field) + (cfg.required.includes(field) ? '*' : '');

    if (cadastro === 'pessoa_cliente' && field === 'cliente_id') {
      const select = document.createElement('select');
      select.name = field;
      select.required = cfg.required.includes(field);

      const empty = document.createElement('option');
      empty.value = '';
      empty.textContent = 'Selecione';
      select.appendChild(empty);

      label.appendChild(select);
    } else {
      const input = document.createElement('input');
      input.name = field;
      input.type = 'text';
      input.required = cfg.required.includes(field);
      label.appendChild(input);
    }

    masterFields.appendChild(label);
  }

  if (cadastro === 'pessoa_cliente') {
    loadPessoaClienteClientesOptions();
  }
}

async function loadPessoaClienteClientesOptions() {
  const select = masterFields.querySelector('[name="cliente_id"]');
  if (!select) return;

  const response = await fetch('/api/cadastros/clientes');
  const rows = await response.json();

  const current = select.value;
  select.innerHTML = '';

  const empty = document.createElement('option');
  empty.value = '';
  empty.textContent = 'Selecione';
  select.appendChild(empty);

  for (const row of rows) {
    const opt = document.createElement('option');
    opt.value = row.id;
    opt.textContent = `${row.id} - ${row.nome ?? ''}`;
    select.appendChild(opt);
  }

  if (current) {
    select.value = current;
  }
}

async function loadMasterRows() {
  const cadastro = masterName.value;
  const cfg = meta[cadastro];
  const listFields = cfg.list_fields || cfg.fields;
  const response = await fetch(`/api/cadastros/${cadastro}`);
  const rows = await response.json();
  currentMasterRows = rows;

  masterTableHeadRow.innerHTML = '';
  for (const key of ['id', ...listFields]) {
    const th = document.createElement('th');
    th.textContent = labelize(key);
    masterTableHeadRow.appendChild(th);
  }
  const actionsTh = document.createElement('th');
  actionsTh.textContent = 'Acoes';
  masterTableHeadRow.appendChild(actionsTh);

  masterTableBody.innerHTML = '';
  for (const row of rows) {
    const tr = document.createElement('tr');
    for (const key of ['id', ...listFields]) {
      const td = document.createElement('td');
      td.textContent = row[key] ?? '';
      tr.appendChild(td);
    }
    const actionsTd = document.createElement('td');
    actionsTd.innerHTML = `<button type="button" class="edit-master-btn" data-id="${row.id}">Editar</button>`;
    tr.appendChild(actionsTd);
    masterTableBody.appendChild(tr);
  }
}

function resetMasterFormMode() {
  const cadastro = masterName.value;
  const cfg = meta[cadastro];

  if (cfg) {
    for (const field of cfg.fields) {
      const input = masterFields.querySelector(`[name="${field}"]`);
      if (input) input.value = '';
    }
  }
  masterRecordId.value = '';
  masterFormTitle.textContent = 'Cadastros Mestres';
  saveMasterBtn.textContent = 'Salvar Cadastro';
  cancelMasterEditBtn.style.display = 'none';
}

function enableMasterEdit(row) {
  const cadastro = masterName.value;
  const cfg = meta[cadastro];

  masterRecordId.value = String(row.id);
  for (const field of cfg.fields) {
    const input = masterFields.querySelector(`[name="${field}"]`);
    if (input) input.value = row[field] ?? '';
  }

  masterFormTitle.textContent = `Editando ${labelize(cadastro)} #${row.id}`;
  saveMasterBtn.textContent = 'Atualizar Cadastro';
  cancelMasterEditBtn.style.display = 'inline-block';
  masterResult.textContent = 'Modo de edicao ativado.';
}

async function saveMaster(event) {
  event.preventDefault();
  const cadastro = masterName.value;
  const cfg = meta[cadastro];
  const editingId = masterRecordId.value;

  const payload = {};
  for (const field of cfg.fields) {
    payload[field] = masterFields.querySelector(`[name="${field}"]`)?.value ?? '';
  }

  const isEdit = !!editingId;
  const endpoint = isEdit ? `/api/cadastros/${cadastro}/${editingId}` : `/api/cadastros/${cadastro}`;
  const method = isEdit ? 'PUT' : 'POST';

  const response = await fetch(endpoint, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await response.json();

  if (!response.ok) {
    masterResult.textContent = data.error || 'Erro ao salvar cadastro.';
    return;
  }

  masterResult.textContent = data.message;

  if (isEdit) {
    resetMasterFormMode();
  } else {
    for (const field of cfg.fields) {
      const input = masterFields.querySelector(`[name="${field}"]`);
      if (input) input.value = '';
    }
  }

  await loadMasterRows();
  await loadFkOptions();
  await loadAllPropostas();
}

async function loadFkOptions() {
  for (const [fkField, cfg] of Object.entries(fkMapping)) {
    const select = document.getElementById(`fk-${fkField}`);
    if (!select) continue;

    const response = await fetch(`/api/cadastros/${cfg.cadastro}`);
    const rows = await response.json();

    select.innerHTML = '';
    const empty = document.createElement('option');
    empty.value = '';
    empty.textContent = 'Selecione';
    select.appendChild(empty);

    for (const row of rows) {
      const opt = document.createElement('option');
      opt.value = row.id;
      opt.textContent = `${row.id} - ${row[cfg.labelField] ?? ''}`;
      select.appendChild(opt);
    }
  }

}

async function saveOportunidade(event) {
  event.preventDefault();

  const formData = new FormData(oppForm);
  const payload = Object.fromEntries(formData.entries());
  const oportunidadeId = payload.oportunidade_id;
  delete payload.oportunidade_id;

  for (const [key, value] of Object.entries(payload)) {
    if (value === '') payload[key] = null;
  }

  const isEdit = !!oportunidadeId;
  const endpoint = isEdit ? `/api/oportunidades/${oportunidadeId}` : '/api/oportunidades';
  const method = isEdit ? 'PUT' : 'POST';

  const response = await fetch(endpoint, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await response.json();

  if (!response.ok) {
    oppResult.textContent = data.error || 'Erro ao salvar oportunidade.';
    return;
  }

  if (isEdit) {
    oppResult.textContent = data.message;
  } else {
    oppResult.textContent = `${data.message} Codigo oportunidade: ${data.id_oportunidade}. Codigo proposta: ${data.codigo_proposta}.`;
  }

  resetOppFormMode();
  await loadAllPropostas();
}

function setSelectValue(name, value) {
  const select = oppForm.querySelector(`[name="${name}"]`);
  if (!select) return;
  select.value = value == null ? '' : String(value);
}

function setInputValue(name, value) {
  const input = oppForm.querySelector(`[name="${name}"]`);
  if (!input) return;
  input.value = value == null ? '' : String(value);
}

function resetOppFormMode() {
  oppForm.reset();
  oportunidadeIdInput.value = '';
  oppFormTitle.textContent = 'Cadastro de Oportunidade';
  saveOppBtn.textContent = 'Salvar Oportunidade';
  cancelEditBtn.style.display = 'none';
}

async function editarOportunidade(oportunidadeId) {
  const response = await fetch(`/api/oportunidades/${oportunidadeId}`);
  const data = await response.json();
  if (!response.ok) {
    oppResult.textContent = data.error || 'Erro ao carregar proposta para edicao.';
    return;
  }

  oportunidadeIdInput.value = data.oportunidade_id;
  setInputValue('data_entrada', data.data_entrada);
  setSelectValue('cliente_id', data.cliente_id);
  setSelectValue('segmento_id', data.segmento_id);
  setSelectValue('decisao_id', data.decisao_id);
  setSelectValue('justificativa_id', data.justificativa_id);
  setSelectValue('canal_id', data.canal_id);
  setSelectValue('responsavel_1_id', data.responsavel_1_id);
  setSelectValue('responsavel_2_id', data.responsavel_2_id);
  setSelectValue('categoria_id', data.categoria_id);
  setSelectValue('area_negocio_id', data.area_negocio_id);
  setSelectValue('estado_id', data.estado_id);
  setInputValue('observacoes_acompanhamento', data.observacoes_acompanhamento);

  updateJustificativaRuleFromDecision();

  oppFormTitle.textContent = `Editando Proposta #${data.codigo_proposta ?? data.oportunidade_id}`;
  saveOppBtn.textContent = 'Atualizar Oportunidade';
  cancelEditBtn.style.display = 'inline-block';
  oppResult.textContent = 'Modo de edicao ativado.';
  oppForm.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function loadAllPropostas() {
  const response = await fetch('/api/oportunidades?limit=5000');
  const rows = await response.json();

  allPropsBody.innerHTML = '';
  currentAllOppRows = rows;
  for (const row of rows) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.oportunidade_id ?? ''}</td>
      <td>${row.id_oportunidade ?? ''}</td>
      <td>${formatVersao(row.versao_atual)}</td>
      <td>${row.data_entrada ?? ''}</td>
      <td>${row.cliente_nome ?? ''}</td>
      <td>${row.segmento_nome ?? ''}</td>
      <td>${row.decisao_nome ?? ''}</td>
      <td>${row.justificativa_nome ?? ''}</td>
      <td>${row.canal_nome ?? ''}</td>
      <td>${row.responsavel_1_nome ?? ''}</td>
      <td>${row.responsavel_2_nome ?? ''}</td>
      <td>${row.categoria_nome ?? ''}</td>
      <td>${row.area_negocio_nome ?? ''}</td>
      <td>${row.uf ?? ''}</td>
      <td>${row.observacoes_acompanhamento ?? ''}</td>
      <td>
        <button type="button" class="decision-opp-btn" data-id="${row.oportunidade_id}">Alterar Decisao</button>
        <button type="button" class="history-opp-btn secondary" data-id="${row.oportunidade_id}" data-cod="${row.codigo_proposta ?? ''}">Historico</button>
      </td>
    `;
    allPropsBody.appendChild(tr);
  }
}

async function loadHistoricoOportunidade(oportunidadeId, codigoProposta) {
  const response = await fetch(`/api/oportunidades/${oportunidadeId}/historico`);
  const rows = await response.json();

  if (!response.ok) {
    oppResult.textContent = rows.error || 'Erro ao carregar historico.';
    return;
  }

  historyTitle.textContent = `Historico de Oportunidades ${codigoProposta || `#${oportunidadeId}`}`;
  historyBody.innerHTML = '';

  if (!rows.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="16">Sem historico de oportunidades para este registro.</td>';
    historyBody.appendChild(tr);
  }

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const previousRow = rows[index + 1] || null;
    const changed = (field) => changedHistoryClass(row, previousRow, field);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${formatVersao(row.versao)}</td>
      <td>${row.snapshot_at ?? ''}</td>
      <td${changed('id_oportunidade')}>${row.id_oportunidade ?? ''}</td>
      <td${changed('codigo_proposta')}>${row.codigo_proposta ?? ''}</td>
      <td${changed('data_entrada')}>${row.data_entrada ?? ''}</td>
      <td${changed('cliente_nome')}>${row.cliente_nome ?? ''}</td>
      <td${changed('segmento_nome')}>${row.segmento_nome ?? ''}</td>
      <td${changed('decisao_nome')}>${row.decisao_nome ?? ''}</td>
      <td${changed('justificativa_nome')}>${row.justificativa_nome ?? ''}</td>
      <td${changed('canal_nome')}>${row.canal_nome ?? ''}</td>
      <td${changed('responsavel_1_nome')}>${row.responsavel_1_nome ?? ''}</td>
      <td${changed('responsavel_2_nome')}>${row.responsavel_2_nome ?? ''}</td>
      <td${changed('categoria_nome')}>${row.categoria_nome ?? ''}</td>
      <td${changed('area_negocio_nome')}>${row.area_negocio_nome ?? ''}</td>
      <td${changed('uf')}>${row.uf ?? ''}</td>
      <td${changed('observacoes_acompanhamento')}>${row.observacoes_acompanhamento ?? ''}</td>
    `;
    historyBody.appendChild(tr);
  }

  historyPanel.style.display = 'block';
  historyPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

allPropsBody.addEventListener('click', async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.classList.contains('decision-opp-btn')) {
    const id = target.dataset.id;
    if (!id) return;
    const row = currentAllOppRows.find((item) => String(item.oportunidade_id) === String(id));
    if (!row) return;
    if (!decisoesCache.length) {
      await loadDecisionPanelOptions();
    }
    openDecisaoPanel(row);
    return;
  }

  if (target.classList.contains('history-opp-btn')) {
    const id = target.dataset.id;
    if (!id) return;
    const cod = target.dataset.cod || '';
    await loadHistoricoOportunidade(id, cod);
  }
});

masterName.addEventListener('change', async () => {
  resetMasterFormMode();
  renderMasterFields();
  await loadMasterRows();
});

masterTableBody.addEventListener('click', (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains('edit-master-btn')) return;

  const id = Number(target.dataset.id);
  if (!id) return;
  const row = currentMasterRows.find((item) => Number(item.id) === id);
  if (!row) return;
  enableMasterEdit(row);
});

masterForm.addEventListener('submit', saveMaster);
oppForm.addEventListener('submit', saveOportunidade);

document.getElementById('refresh-master-btn').addEventListener('click', loadMasterRows);
document.getElementById('refresh-all-props').addEventListener('click', loadAllPropostas);
cancelMasterEditBtn.addEventListener('click', () => {
  resetMasterFormMode();
  masterResult.textContent = 'Edicao cancelada.';
});
cancelEditBtn.addEventListener('click', () => {
  resetOppFormMode();
  oppResult.textContent = 'Edicao cancelada.';
});
closeHistoryBtn.addEventListener('click', () => {
  historyPanel.style.display = 'none';
});

decisaoUpdateSelect.addEventListener('change', updateJustificativaRuleFromDecisionPanel);
closeDecisaoBtn.addEventListener('click', closeDecisaoPanel);
cancelDecisaoBtn.addEventListener('click', closeDecisaoPanel);
decisaoForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const oportunidadeId = decisaoOportunidadeId.value;
  if (!oportunidadeId) return;

  const payload = {
    decisao_id: decisaoUpdateSelect.value || null,
    justificativa_id: justificativaUpdateSelect.value || null,
  };

  const response = await fetch(`/api/oportunidades/${oportunidadeId}/decisao`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    decisaoResult.textContent = data.error || 'Erro ao atualizar decisao.';
    return;
  }

  decisaoResult.textContent = data.message || 'Decisao atualizada com sucesso.';
  await loadAllPropostas();
});

async function bootstrap() {
  await loadMeta();
  await loadMasterRows();
  await loadFkOptions();
  await loadDecisionPanelOptions();
  await loadAllPropostas();

  const params = new URLSearchParams(window.location.search);
  const editId = params.get('edit_oportunidade');
  if (editId) {
    await editarOportunidade(editId);
  }
}

bootstrap();
