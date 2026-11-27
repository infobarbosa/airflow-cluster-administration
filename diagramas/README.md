# Diagramas

Cada diagrama do roteiro existe em dois formatos, com o mesmo nome:

- **`.svg`** — a imagem que aparece no README.
- **`.excalidraw`** — a cena editável. Abra em [excalidraw.com](https://excalidraw.com) (menu **Abrir**, ou arraste o arquivo para a página) para editar ou desenhar por cima durante a aula.

Para atualizar uma imagem depois de editar a cena: **Exportar imagem → SVG**, com fundo, e salve por cima do `.svg` de mesmo nome. Salve também a cena (`.excalidraw`), para as duas versões não divergirem.

| Arquivo | Seção |
|---|---|
| `01-cluster-do-laboratorio` | O ambiente — os 10 serviços do cluster |
| `02-celery-executor` | 4 — O caminho de uma task no CeleryExecutor |
| `03-falhas-e-recuperacao` | Parte 3 — O que acontece quando cada componente cai |
| `04-laboratorio-vs-producao` | 14 — Laboratório × docker-compose × Helm chart |

## Acessibilidade

Os diagramas seguem três regras, para continuarem legíveis no GitHub (que reduz imagens largas à largura da coluna, cerca de 880 px) e para pessoas com baixa visão:

- **Largura máxima de ~1.100 px**: acima disso, o GitHub reduz a imagem e o texto encolhe junto.
- **Fonte Nunito** (no Excalidraw: *Normal*), não a manuscrita, com **no mínimo 20 px** (rótulos de 22 a 24 px, títulos de 34 px).
- **Contraste mínimo de 4,5:1** (WCAG AA) entre o texto e o fundo.

Ao editar uma cena, mantenha essas regras.
