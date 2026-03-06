const goOppsBody = document.getElementById('go-opps-body');
const refreshGoOppsBtn = document.getElementById('refresh-go-opps');
const goHistoryPanel = document.getElementById('go-history-panel');
const goHistoryTitle = document.getElementById('go-history-title');
const goHistoryBody = document.getElementById('go-history-body');
const closeGoHistoryBtn = document.getElementById('close-go-history-btn');
const goViewModal = document.getElementById('go-view-modal');
const goViewTitle = document.getElementById('go-view-title');
const goViewBody = document.getElementById('go-view-body');
const closeGoViewBtn = document.getElementById('close-go-view-btn');
const goEditPanel = document.getElementById('go-edit-panel');
const goOppsHeadRow = document.getElementById('go-opps-head-row');
const goHistoryHeadRow = document.getElementById('go-history-head-row');
const goEditTitle = document.getElementById('go-edit-title');
const goEditForm = document.getElementById('go-edit-form');
const goEditResult = document.getElementById('go-edit-result');
const cancelGoEditBtn = document.getElementById('cancel-go-edit-btn');
const cancelGoEditBtn2 = document.getElementById('cancel-go-edit-btn-2');
const goEditStatusPropostaSelect = document.getElementById('go-edit-status-proposta-id');
const goEditResponsavel1Select = document.getElementById('go-edit-responsavel-1-id');
const goEditResponsavel2Select = document.getElementById('go-edit-responsavel-2-id');
const goEditDateFieldWrap = document.getElementById('go-edit-date-field-wrap');
const goEditDateFieldLabel = document.getElementById('go-edit-date-field-label');
const goEditDataEnvioInput = document.getElementById('go-edit-data-envio');

let goRowsById = new Map();
let statusPropostaRows = [];
let currentEditStatusDatas = {};
let currentEditDataEnvio = '';
let activeDateSource = '';

function formatCurrencyBRL(value) {
  if (value == null || value === '') return '';
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
  }).format(numeric);
}

function formatVersao(value) {
  if (value == null) return '';
  const text = String(value).trim();
  if (!text) return '';
  if (/^v\d+$/i.test(text)) return `V${Number.parseInt(text.slice(1), 10)}`;
  const numeric = Number.parseInt(text, 10);
  if (!Number.isFinite(numeric) || numeric <= 0) return '';
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

async function loadStatusPropostaOptions() {
  const response = await fetch('/api/cadastros/status_proposta');
  const rows = await response.json();
  statusPropostaRows = rows;

  goEditStatusPropostaSelect.innerHTML = '';
  const empty = document.createElement('option');
  empty.value = '';
  empty.textContent = 'Selecione';
  goEditStatusPropostaSelect.appendChild(empty);

  for (const row of rows) {
    const opt = document.createElement('option');
    opt.value = row.id;
    opt.textContent = `${row.id} - ${row.nome ?? ''}`;
    goEditStatusPropostaSelect.appendChild(opt);
  }

  renderGoTableHeaders();
}

function isDataEnvioStatus(statusNome) {
  const normalized = String(statusNome || '').trim().toUpperCase();
  return normalized.includes('ENVIO') || normalized.includes('ENVIADO') || normalized.includes('ENVIADA');
}

function getStatusDateStatuses() {
  return statusPropostaRows.filter((statusRow) => !isDataEnvioStatus(statusRow.nome));
}

function getStatusById(statusId) {
  return statusPropostaRows.find((statusRow) => String(statusRow.id) === String(statusId)) || null;
}

function renderGoTableHeaders() {
  if (!goOppsHeadRow || !goHistoryHeadRow) return;

  const statusDateStatuses = getStatusDateStatuses();
  const statusHeaders = statusDateStatuses
    .map((statusRow) => `<th>Data_${statusRow.nome ?? statusRow.id}</th>`)
    .join('');

  goOppsHeadRow.innerHTML = `
    <th>CODIGO_PROPOSTA</th>
    <th>VERSAO</th>
    <th>CLIENTE</th>
    <th>OBJETO</th>
    <th>STATUS_Proposta</th>
    <th>Valor_Proposta</th>
    <th>Data_Envio</th>
    <th>Responsavel</th>
    <th>Canal</th>
    <th>Observacao</th>
    ${statusHeaders}
    <th>Acoes</th>
  `;

  goHistoryHeadRow.innerHTML = `
    <th>CODIGO_PROPOSTA</th>
    <th>VERSAO</th>
    <th>Snapshot Em</th>
    <th>CLIENTE</th>
    <th>OBJETO</th>
    <th>STATUS_Proposta</th>
    <th>Valor_Proposta</th>
    <th>Data_Envio</th>
    <th>Responsavel</th>
    <th>Canal</th>
    <th>Observacao</th>
    ${statusHeaders}
  `;
}

function renderStatusDateCells(statusDatas) {
  const current = statusDatas && typeof statusDatas === 'object' ? statusDatas : {};
  const statusDateStatuses = getStatusDateStatuses();
  return statusDateStatuses
    .map((statusRow) => {
      const statusId = String(statusRow.id);
      return `<td>${current[statusId] ?? ''}</td>`;
    })
    .join('');
}

function renderStatusDateCellsWithChanges(row, previousRow) {
  const current = row && typeof row.status_datas === 'object' ? row.status_datas : {};
  if (!previousRow) {
    return renderStatusDateCells(current);
  }
  const previous = previousRow && typeof previousRow.status_datas === 'object' ? previousRow.status_datas : {};
  const statusDateStatuses = getStatusDateStatuses();
  return statusDateStatuses
    .map((statusRow) => {
      const statusId = String(statusRow.id);
      const currentValue = current[statusId] ?? '';
      const previousValue = previous[statusId] ?? '';
      const className = currentValue !== previousValue ? ' class="history-changed"' : '';
      return `<td${className}>${currentValue}</td>`;
    })
    .join('');
}

function renderStatusDateFields(statusDatas) {
  currentEditStatusDatas = statusDatas && typeof statusDatas === 'object' ? { ...statusDatas } : {};
}

function persistCurrentDateValue() {
  if (!goEditDataEnvioInput) return;
  const value = goEditDataEnvioInput.value || null;
  if (activeDateSource === 'envio') {
    currentEditDataEnvio = value || '';
    return;
  }
  if (activeDateSource) {
    currentEditStatusDatas[activeDateSource] = value;
  }
}

function syncSharedDateFieldByStatus() {
  if (!goEditStatusPropostaSelect || !goEditDataEnvioInput || !goEditDateFieldWrap || !goEditDateFieldLabel) return;

  persistCurrentDateValue();

  const selectedStatusId = String(goEditStatusPropostaSelect.value || '').trim();
  if (!selectedStatusId) {
    activeDateSource = '';
    goEditDateFieldWrap.style.display = 'none';
    goEditDataEnvioInput.value = '';
    return;
  }

  const selectedStatus = getStatusById(selectedStatusId);
  if (!selectedStatus) {
    activeDateSource = '';
    goEditDateFieldWrap.style.display = 'none';
    goEditDataEnvioInput.value = '';
    return;
  }

  goEditDateFieldWrap.style.display = '';
  if (isDataEnvioStatus(selectedStatus.nome)) {
    activeDateSource = 'envio';
    goEditDateFieldLabel.textContent = 'Data_Envio';
    goEditDataEnvioInput.value = currentEditDataEnvio || '';
    return;
  }

  activeDateSource = String(selectedStatus.id);
  goEditDateFieldLabel.textContent = `Data - ${selectedStatus.nome ?? `Status ${selectedStatus.id}`}`;
  goEditDataEnvioInput.value = currentEditStatusDatas[activeDateSource] || '';
}

async function loadResponsavelOptions() {
  const response = await fetch('/api/cadastros/resposaveis_pessoaTema');
  const rows = await response.json();

  goEditResponsavel1Select.innerHTML = '';
  const empty1 = document.createElement('option');
  empty1.value = '';
  empty1.textContent = 'Selecione';
  goEditResponsavel1Select.appendChild(empty1);

  goEditResponsavel2Select.innerHTML = '';
  const empty = document.createElement('option');
  empty.value = '';
  empty.textContent = 'Selecione';
  goEditResponsavel2Select.appendChild(empty);

  for (const row of rows) {
    const opt1 = document.createElement('option');
    opt1.value = row.id;
    opt1.textContent = `${row.id} - ${row.nome ?? ''}`;
    goEditResponsavel1Select.appendChild(opt1);

    const opt = document.createElement('option');
    opt.value = row.id;
    opt.textContent = `${row.id} - ${row.nome ?? ''}`;
    goEditResponsavel2Select.appendChild(opt);
  }
}

function formatResponsaveis(responsavel1, responsavel2) {
  if (responsavel1 && responsavel2) return `${responsavel1} / ${responsavel2}`;
  return responsavel1 || responsavel2 || '';
}

function escapeHtml(value) {
  const text = value == null ? '' : String(value);
  return text
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function renderDetailItem(label, value) {
  return `
    <div class="proposal-view-item">
      <p class="proposal-view-label">${escapeHtml(label)}</p>
      <p class="proposal-view-value">${escapeHtml(value || '-')}</p>
    </div>
  `;
}

function openViewPanel(row) {
  if (!goViewModal || !goViewBody || !goViewTitle) return;

  const statusDateStatuses = getStatusDateStatuses();
  const statusDatesHtml = statusDateStatuses
    .map((statusRow) => {
      const statusId = String(statusRow.id);
      const dateValue = (row.status_datas && row.status_datas[statusId]) ? row.status_datas[statusId] : '-';
      return renderDetailItem(`Data - ${statusRow.nome ?? statusId}`, dateValue);
    })
    .join('');

  goViewTitle.textContent = `Detalhes ${row.codigo_proposta ?? `#${row.oportunidade_id}`}`;
  goViewBody.innerHTML = `
    ${renderDetailItem('Codigo Proposta', row.codigo_proposta)}
    ${renderDetailItem('Versao', row.versao_proposta_atual)}
    ${renderDetailItem('Cliente', row.cliente_nome)}
    ${renderDetailItem('Objeto', row.objeto)}
    ${renderDetailItem('Status Proposta', row.status_proposta_nome)}
    ${renderDetailItem('Valor Proposta', formatCurrencyBRL(row.valor))}
    ${renderDetailItem('Data Envio', row.data_envio)}
    ${renderDetailItem('Responsavel', formatResponsaveis(row.responsavel_1_nome, row.responsavel_2_nome))}
    ${renderDetailItem('Canal', row.canal_nome)}
    ${renderDetailItem('Observacao', row.observacao_proposta)}
    ${statusDatesHtml}
  `;

  goViewModal.style.display = 'flex';
}

function closeViewPanel() {
  if (!goViewModal || !goViewBody) return;
  goViewModal.style.display = 'none';
  goViewBody.innerHTML = '';
}

function setFormValue(name, value) {
  const field = goEditForm.querySelector(`[name="${name}"]`);
  if (!field) return;
  field.value = value == null ? '' : String(value);
}

function openEditPanel(row) {
  setFormValue('oportunidade_id', row.oportunidade_id);
  setFormValue('status_proposta_id', row.status_proposta_id);
  setFormValue('valor', row.valor);
  currentEditDataEnvio = row.data_envio || '';
  setFormValue('data_envio', currentEditDataEnvio);
  setFormValue('responsavel_1_id', row.responsavel_1_id);
  setFormValue('responsavel_2_id', row.responsavel_2_id);
  setFormValue('observacao_proposta', row.observacao_proposta);
  renderStatusDateFields(row.status_datas);
  activeDateSource = '';
  syncSharedDateFieldByStatus();

  goEditTitle.textContent = `Editar Proposta ${row.codigo_proposta ?? `#${row.oportunidade_id}`}`;
  goEditResult.textContent = '';
  goEditPanel.style.display = 'block';
  goEditPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function closeEditPanel() {
  goEditForm.reset();
  goEditResult.textContent = '';
  currentEditDataEnvio = '';
  currentEditStatusDatas = {};
  activeDateSource = '';
  if (goEditDateFieldWrap) {
    goEditDateFieldWrap.style.display = 'none';
  }
  goEditPanel.style.display = 'none';
}

async function loadGoOportunidades() {
  const response = await fetch('/api/oportunidades/go?limit=5000');
  const rows = await response.json();

  goRowsById = new Map(rows.map((row) => [String(row.oportunidade_id), row]));
  goOppsBody.innerHTML = '';
  for (const row of rows) {
    const tr = document.createElement('tr');
    const versao = formatVersao(row.versao_proposta_atual);
    const statusDateCells = renderStatusDateCells(row.status_datas);
    tr.innerHTML = `
      <td>${row.codigo_proposta ?? ''}</td>
      <td>${versao}</td>
      <td>${row.cliente_nome ?? ''}</td>
      <td>${row.objeto ?? ''}</td>
      <td>${row.status_proposta_nome ?? ''}</td>
      <td>${formatCurrencyBRL(row.valor)}</td>
      <td>${row.data_envio ?? ''}</td>
      <td>${formatResponsaveis(row.responsavel_1_nome, row.responsavel_2_nome)}</td>
      <td>${row.canal_nome ?? ''}</td>
      <td>${row.observacao_proposta ?? ''}</td>
      ${statusDateCells}
      <td>
        <button type="button" class="view-go-opp-btn secondary" data-id="${row.oportunidade_id}" aria-label="Ver detalhes">&#128065;</button>
        <button type="button" class="edit-go-opp-btn" data-id="${row.oportunidade_id}">Editar</button>
        <button type="button" class="history-go-opp-btn secondary" data-id="${row.oportunidade_id}" data-cod="${row.codigo_proposta ?? ''}">Historico</button>
      </td>
    `;
    goOppsBody.appendChild(tr);
  }
}

async function loadHistoricoOportunidade(oportunidadeId, codigoProposta) {
  const response = await fetch(`/api/propostas/${oportunidadeId}/historico`);
  const rows = await response.json();

  if (!response.ok) return;

  goHistoryTitle.textContent = `Historico de Propostas ${codigoProposta || `#${oportunidadeId}`}`;
  goHistoryBody.innerHTML = '';

  if (!rows.length) {
    const tr = document.createElement('tr');
    const colspan = 11 + getStatusDateStatuses().length;
    tr.innerHTML = `<td colspan="${colspan}">Sem historico de propostas para este registro.</td>`;
    goHistoryBody.appendChild(tr);
  }

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const previousRow = rows[index + 1] || null;
    const changed = (field) => changedHistoryClass(row, previousRow, field);
    const responsaveisChanged = previousRow
      && (
        normalizeHistoryValue(row.responsavel_1_nome) !== normalizeHistoryValue(previousRow.responsavel_1_nome)
        || normalizeHistoryValue(row.responsavel_2_nome) !== normalizeHistoryValue(previousRow.responsavel_2_nome)
      );
    const responsaveisClass = responsaveisChanged ? ' class="history-changed"' : '';
    const tr = document.createElement('tr');
    const versao = formatVersao(row.versao);
    const statusDateCells = renderStatusDateCellsWithChanges(row, previousRow);
    tr.innerHTML = `
      <td${changed('codigo_proposta')}>${row.codigo_proposta ?? ''}</td>
      <td>${versao}</td>
      <td>${row.snapshot_at ?? ''}</td>
      <td${changed('cliente_nome')}>${row.cliente_nome ?? ''}</td>
      <td${changed('objeto')}>${row.objeto ?? ''}</td>
      <td${changed('status_proposta_nome')}>${row.status_proposta_nome ?? ''}</td>
      <td${changed('valor')}>${formatCurrencyBRL(row.valor)}</td>
      <td${changed('data_envio')}>${row.data_envio ?? ''}</td>
      <td${responsaveisClass}>${formatResponsaveis(row.responsavel_1_nome, row.responsavel_2_nome)}</td>
      <td${changed('canal_nome')}>${row.canal_nome ?? ''}</td>
      <td${changed('observacao_proposta')}>${row.observacao_proposta ?? ''}</td>
      ${statusDateCells}
    `;
    goHistoryBody.appendChild(tr);
  }

  goHistoryPanel.style.display = 'block';
  goHistoryPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

goOppsBody.addEventListener('click', async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.classList.contains('view-go-opp-btn')) {
    const id = target.dataset.id;
    if (!id) return;
    const row = goRowsById.get(String(id));
    if (!row) return;
    openViewPanel(row);
    return;
  }

  if (target.classList.contains('edit-go-opp-btn')) {
    const id = target.dataset.id;
    if (!id) return;
    const row = goRowsById.get(String(id));
    if (!row) return;
    openEditPanel(row);
    return;
  }

  if (target.classList.contains('history-go-opp-btn')) {
    const id = target.dataset.id;
    if (!id) return;
    const cod = target.dataset.cod || '';
    await loadHistoricoOportunidade(id, cod);
  }
});

closeGoHistoryBtn.addEventListener('click', () => {
  goHistoryPanel.style.display = 'none';
});

closeGoViewBtn.addEventListener('click', closeViewPanel);
goViewModal.addEventListener('click', (event) => {
  if (event.target === goViewModal) {
    closeViewPanel();
  }
});

cancelGoEditBtn.addEventListener('click', closeEditPanel);
cancelGoEditBtn2.addEventListener('click', closeEditPanel);

goEditForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const formData = new FormData(goEditForm);
  const oportunidadeId = formData.get('oportunidade_id');
  if (!oportunidadeId) return;

  const payload = {
    status_proposta_id: formData.get('status_proposta_id'),
    responsavel_1_id: formData.get('responsavel_1_id'),
    responsavel_2_id: formData.get('responsavel_2_id'),
    valor: formData.get('valor'),
    data_envio: null,
    observacao_proposta: formData.get('observacao_proposta'),
    status_datas: { ...currentEditStatusDatas },
  };

  persistCurrentDateValue();
  payload.data_envio = currentEditDataEnvio || null;

  for (const [key, value] of Object.entries(payload)) {
    if (value === '') payload[key] = null;
  }

  const response = await fetch(`/api/propostas/${oportunidadeId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await response.json();

  if (!response.ok) {
    goEditResult.textContent = data.error || 'Erro ao atualizar proposta.';
    return;
  }

  goEditResult.textContent = data.message || 'Proposta atualizada com sucesso.';
  await loadGoOportunidades();
});

refreshGoOppsBtn.addEventListener('click', loadGoOportunidades);
goEditStatusPropostaSelect.addEventListener('change', () => {
  syncSharedDateFieldByStatus();
});
async function bootstrap() {
  await loadStatusPropostaOptions();
  await loadResponsavelOptions();
  await loadGoOportunidades();
}

bootstrap();
