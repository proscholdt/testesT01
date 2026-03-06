# ERP CRM Local - System Context (for future sessions)

## Stack
- Backend: Flask (`app.py`)
- DB: SQLite (`data/crm.db`)
- Frontend: server-rendered HTML + vanilla JS + CSS
  - Tela 1: `templates/index.html`, `static/app.js`
  - Tela 2 (Propostas GO): `templates/propostas.html`, `static/propostas.js`

## Core Domain
- Main table: `oportunidades_erp`
- History table: `oportunidades_erp_historico`
- Master tables for FKs: clientes, pessoa_cliente, segmentos, decisoes, justificativas, canais, responsaveis, categorias, areas_negocio, estados, status_proposta

## Important Business Rules
- New oportunidade starts with:
  - `decisao_id = NULL`
  - `justificativa_id = NULL`
  - `versao_atual = 1`
- Justificativa is required only for NO-GO decisions.
- First transition to GO must remain V1 (no version bump on decision change).
- Proposal field edits (Tela 2) increment version.
- Proposal history must include V1 at GO transition moment.

## Two-Screen Behavior
- Tela 1 (`/`): manage opportunities + decision change action.
- Tela 2 (`/propostas`): list/edit GO proposals and proposal-specific fields.
- `Responsavel2` is available in Tela 2 edit form and saved via proposal update endpoint.

## History Separation (critical)
- History rows use marker `origem_historico`:
  - `oportunidade` for Tela 1 history
  - `proposta` for Tela 2 history
- Endpoints filter by origin:
  - `/api/oportunidades/<id>/historico` -> only `origem_historico = 'oportunidade'`
  - `/api/propostas/<id>/historico` -> only `origem_historico = 'proposta'`

## Key Endpoints
- `GET /api/oportunidades`
- `POST /api/oportunidades`
- `PUT /api/oportunidades/<id>/decisao`
- `GET /api/oportunidades/go`
- `PUT /api/propostas/<id>`
- `GET /api/oportunidades/<id>/historico`
- `GET /api/propostas/<id>/historico`

## Proposal Screen Fields (current focus)
- Columns/history order expected by user:
  - CODIGO_PROPOSTA, VERSAO, CLIENTE, OBJETO, STATUS_Proposta, Valor_Proposta, Data_Envio, Responsavel, Tipo_Evento, Data_Evento, Valor_Evento, Canal, Observacao, Nome_Contato, Proxima_Acao, Farol, Evento_Encerrador
- `Responsavel` display combines names when both exist (`Responsavel1 / Responsavel2`).

## Recurring Operational Request
- User frequently requests DB cleanup (clear opportunities + history + reset sequences).

## Known Risk / Verification Checklist
- If history looks wrong, verify both:
  - origin filter in backend endpoint
  - frontend column mapping order in `static/propostas.js`
- If V1 not appearing in proposal history, validate GO transition flow in `PUT /api/oportunidades/<id>/decisao` and baseline insert with `origem_historico='proposta'`.
