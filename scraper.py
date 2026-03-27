# -*- coding: utf-8 -*-
"""
Scraper Zap Imoveis — venda RJ (curl_cffi + segmentacao por quartos e preco).

Busca TODOS os imoveis residenciais (apartamento, studio, kitnet, flat, loft)
do Rio de Janeiro, segmentando por quantidade de quartos e faixas de preco
para contornar o limite de paginacao do ZapImoveis.

Usa curl_cffi para impersonar a impressao digital TLS de um Chrome real,
bypassando Cloudflare sem precisar de browser.

Dependencias:
    pip install curl_cffi pandas numpy beautifulsoup4 openpyxl tqdm
"""
from __future__ import annotations

import argparse
import json
import random
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi.requests import Session
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

BASE_URL = "https://www.zapimoveis.com.br/venda/imoveis/rj+rio-de-janeiro/"
START_URL_BASE = BASE_URL.rstrip("/") + "/"
OUTPUT_XLSX = Path(__file__).resolve().parent / "venda_rj_rio_scraped.xlsx"
SAVE_INTERVAL = 50

TIPOS = (
    "apartamento_residencial,"
    "studio_residencial,"
    "kitnet_residencial,"
    "flat_residencial,"
    "loft_residencial"
)

QUARTOS = [1, 2, 3, 4]

FAIXAS_PRECO = [
    (100_000, 275_000),
    (275_001, 400_000),
    (400_001, 600_000),
]

# O ZapImóveis retorna 404 acima da última página (~50 por busca).
MAX_PAGINAS_POR_SEGMENTO = 50
# Delays entre requests (seg) — usados na estimativa de tempo
_DELAY_MIN = 3.0
_DELAY_MAX = 8.0


COLUNAS_FINAIS = [
    "web-scraper-order",
    "web-scraper-start-url",
    "segmento",
    "título",
    "tipo de imóvel",
    "preço total",
    "metragem",
    "endereço",
    "bairro",
    "cidade",
    "estado",
    "tipologia",
    "suítes",
    "banheiros",
    "vagas",
    "andar",
    "Cond",
    "IPTU",
    "descrição",
    "comodidades",
    "código do imóvel",
    "imobiliária",
    "aceita pet",
    "mobiliado",
    "fotos",
    "preço abaixo do mercado",
    "link-href",
    "R$/m2",
]

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# ---------------------------------------------------------------------------
# Sessao e requests
# ---------------------------------------------------------------------------


def _create_session(proxy: str | None = None) -> Session:
    kwargs: dict = {"impersonate": "chrome", "timeout": 30}
    if proxy:
        kwargs["proxies"] = {"https": proxy, "http": proxy}
    session = Session(**kwargs)
    session.headers.update(_HEADERS)
    return session


def _is_blocked(text: str) -> bool:
    low = text[:5000].lower()
    return (
        "why have i been blocked" in low
        or ("cloudflare" in low and "security service" in low)
        or "attention required" in low
        or "you have been blocked" in low
        or "sorry, you have been blocked" in low
    )


def _fetch(
    session: Session,
    url: str,
    *,
    max_retries: int = 3,
    listing_page: bool = False,
) -> str | None:
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url)
            if resp.status_code == 404:
                if listing_page:
                    tqdm.write(
                        "      HTTP 404 — fim da paginação (limite do Zap ~50 págs/busca)."
                    )
                return None
            if resp.status_code == 403 or _is_blocked(resp.text):
                print(f"      Bloqueado (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    backoff = random.uniform(10, 30) * attempt
                    print(f"      Backoff {backoff:.0f}s...")
                    time.sleep(backoff)
                    continue
                return None
            if resp.status_code >= 400:
                print(f"      HTTP {resp.status_code} (attempt {attempt})")
                if attempt < max_retries:
                    time.sleep(random.uniform(5, 15))
                    continue
                return None
            return resp.text
        except Exception as e:
            print(f"      Erro (attempt {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(random.uniform(5, 15))
                continue
            return None
    return None


# ---------------------------------------------------------------------------
# Construcao de URLs de busca
# ---------------------------------------------------------------------------


def _format_preco(valor: int) -> str:
    if valor >= 1_000_000:
        return f"{valor / 1_000_000:.1f}M".replace(".0M", "M")
    return f"{valor // 1000}k"


def _build_search_url(quartos: int, preco_min: int, preco_max: int, pagina: int) -> str:
    params = [
        f"tipos={TIPOS}",
        f"quartos={quartos}",
        f"precoMinimo={preco_min}",
        f"precoMaximo={preco_max}",
        f"pagina={pagina}",
    ]
    return BASE_URL + "?" + "&".join(params)


def _segment_label(quartos: int, preco_min: int, preco_max: int) -> str:
    q = f"{quartos}q" if quartos < 4 else "4+q"
    return f"{q} {_format_preco(preco_min)}-{_format_preco(preco_max)}"


# ---------------------------------------------------------------------------
# Parsing e extracao
# ---------------------------------------------------------------------------


def clean_text(text: str | None) -> str | None:
    return re.sub(r"\s+", " ", text).strip() if text else None


def extract_number(text: str | None) -> float:
    if not text:
        return np.nan
    cleaned = re.sub(r"[^0-9,.]", "", text).strip()
    if cleaned.endswith(",") or cleaned.endswith("."):
        cleaned = cleaned[:-1]
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "." in cleaned and "," not in cleaned:
        if cleaned.count(".") > 1:
            cleaned = cleaned.replace(".", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        if "--" in cleaned or not cleaned:
            return np.nan
        return float(cleaned)
    except ValueError:
        return np.nan


def extract_detail_jsonld(detail_soup: BeautifulSoup) -> dict | None:
    for tag in detail_soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(data, dict):
            continue
        if data.get("@type") != "Product":
            continue
        offers = data.get("offers", {}) or {}
        price = offers.get("price")
        image = data.get("image")
        num_fotos = 0
        if isinstance(image, list):
            num_fotos = len(image)
        elif isinstance(image, str):
            num_fotos = 1
        return {
            "price": float(price) if price is not None else None,
            "description": data.get("description"),
            "name": data.get("name"),
            "sku": data.get("sku"),
            "url": offers.get("url"),
            "num_fotos": num_fotos,
        }
    return None


def extract_all_from_detail(detail_soup: BeautifulSoup) -> dict:
    """Extrai todos os dados possíveis de uma página de detalhe (venda)."""
    jsonld = extract_detail_jsonld(detail_soup)
    jd = jsonld or {}
    result: dict = {}

    _TIPOS = [
        "apartamento", "casa", "cobertura", "flat", "kitnet",
        "studio", "loft", "terreno", "sala", "loja", "galpão", "sobrado",
    ]

    h1 = detail_soup.find("h1")
    result["título"] = clean_text(h1.text) if h1 else jd.get("name")

    tipo = None
    bc = detail_soup.find(class_=re.compile(r"breadcrumb", re.I))
    if bc:
        bt = (bc.get_text() or "").lower()
        for k in _TIPOS:
            if k in bt:
                tipo = k.capitalize()
                break
    if not tipo and result.get("título"):
        tl = result["título"].lower()
        for k in _TIPOS:
            if k in tl:
                tipo = k.capitalize()
                break
    result["tipo de imóvel"] = tipo

    addr_el = detail_soup.find("p", {"data-testid": "location-address"})
    addr_full = clean_text(addr_el.text) if addr_el else ""
    parts = [p.strip() for p in addr_full.split(" - ")] if addr_full else []
    result["endereço"] = parts[0] if parts else np.nan
    result["bairro"] = result["cidade"] = result["estado"] = np.nan
    if len(parts) >= 3:
        cp = [p.strip() for p in parts[1].split(",")]
        result["bairro"] = cp[0]
        if len(cp) > 1:
            result["cidade"] = cp[-1]
        result["estado"] = parts[-1]
    elif len(parts) == 2:
        cp = [p.strip() for p in parts[1].split(",")]
        result["bairro"] = cp[0]
        if len(cp) > 1:
            result["cidade"] = cp[-1]

    vals: dict[str, str] = {}
    for p_title in detail_soup.find_all("p", class_="value-item__title"):
        p_val = p_title.find_next_sibling("p", class_="value-item__value")
        if p_val:
            k = (clean_text(p_title.text) or "").lower()
            v = clean_text(p_val.text)
            if k and v:
                vals[k] = v

    preco = None
    for kp in ["venda", "valor"]:
        for k, v in vals.items():
            if kp in k:
                preco = v
                break
        if preco:
            break
    if not preco and vals:
        preco = next(iter(vals.values()))
    if preco is None and jd.get("price"):
        preco = str(jd["price"])
    result["preço total"] = extract_number(preco)

    cond_el = detail_soup.find("p", {"data-testid": "condoFee"})
    cond_str = clean_text(cond_el.text) if cond_el else None
    if not cond_str:
        for k, v in vals.items():
            if "cond" in k:
                cond_str = v
                break
    cn = extract_number(cond_str)
    result["Cond"] = f"Cond. R$ {int(cn)}" if pd.notna(cn) else np.nan

    iptu_el = detail_soup.find("p", {"data-testid": "iptu"})
    iptu_str = clean_text(iptu_el.text) if iptu_el else None
    if not iptu_str:
        for k, v in vals.items():
            if "iptu" in k:
                iptu_str = v
                break
    ip_num = extract_number(iptu_str)
    result["IPTU"] = f"IPTU R$ {int(ip_num)}" if pd.notna(ip_num) else np.nan

    met = tip = sui = ban = vag = andar_v = None
    feat_ul = detail_soup.find("ul", class_=re.compile(r"amenities"))
    if feat_ul:
        for li in feat_ul.find_all("li"):
            te = li.find("p", class_=re.compile(r"text-1-5"))
            ve = li.find("p", class_=re.compile(r"text-1-75"))
            if not te or not ve:
                continue
            t = (clean_text(te.text) or "").lower()
            v = clean_text(ve.text)
            if any(x in t for x in ["metragem", "área", "m²"]):
                met = v
            elif "suíte" in t or "suite" in t:
                sui = v
            elif "quarto" in t:
                tip = v
            elif "banheiro" in t:
                ban = v
            elif "vaga" in t:
                vag = v
            elif "andar" in t:
                andar_v = v
    result["metragem"] = extract_number(met)
    result["tipologia"] = extract_number(tip)
    result["suítes"] = extract_number(sui)
    result["banheiros"] = extract_number(ban)
    result["vagas"] = extract_number(vag)
    result["andar"] = clean_text(andar_v) if andar_v else np.nan

    desc_el = detail_soup.find("p", {"data-testid": "description-content"})
    result["descrição"] = (
        clean_text(desc_el.get_text(separator="\n")) if desc_el else jd.get("description")
    )

    result["código do imóvel"] = jd.get("sku")

    imob = None
    for pat in ["advertiser", "publisher", "realtor", "agency"]:
        el = detail_soup.find(attrs={"data-testid": re.compile(pat, re.I)})
        if el:
            cand = clean_text(el.get_text())
            if cand and len(cand) < 150:
                imob = cand
                break
    if not imob:
        for pat in ["advertiser", "publisher", "realtor"]:
            sec = detail_soup.find(class_=re.compile(pat, re.I))
            if sec:
                ne = sec.find(["h2", "h3", "p", "span", "a"])
                if ne:
                    cand = clean_text(ne.get_text())
                    if cand and len(cand) < 150:
                        imob = cand
                        break
    result["imobiliária"] = imob or np.nan

    comods: list[str] = []
    for sec in detail_soup.find_all(
        ["ul", "div", "section"],
        class_=re.compile(r"feature|amenit|characteristic|facilit|tag-list", re.I),
    ):
        if sec.find_parent(class_=re.compile(r"amenities-scrollbar")):
            continue
        for item in sec.find_all("li"):
            txt = clean_text(item.get_text())
            if txt and 2 <= len(txt) <= 80:
                comods.append(txt)
    seen: set[str] = set()
    unique: list[str] = []
    for c in comods:
        cl = c.lower()
        if cl not in seen:
            seen.add(cl)
            unique.append(c)
    result["comodidades"] = "; ".join(unique) if unique else np.nan

    page_lower = detail_soup.get_text().lower()
    if "não aceita pet" in page_lower or "nao aceita pet" in page_lower:
        result["aceita pet"] = "Não"
    elif "aceita pet" in page_lower or "pet friendly" in page_lower:
        result["aceita pet"] = "Sim"
    else:
        result["aceita pet"] = np.nan

    if "semi-mobiliado" in page_lower or "semi mobiliado" in page_lower:
        result["mobiliado"] = "Semi-mobiliado"
    elif (
        "não mobiliado" in page_lower
        or "nao mobiliado" in page_lower
        or "sem mobília" in page_lower
    ):
        result["mobiliado"] = "Não"
    elif "mobiliado" in page_lower:
        result["mobiliado"] = "Sim"
    else:
        result["mobiliado"] = np.nan

    fotos = jd.get("num_fotos", 0)
    if fotos == 0:
        gal = detail_soup.find(
            class_=re.compile(r"carousel|gallery|slider|photo", re.I)
        )
        if gal:
            fotos = len(gal.find_all("img"))
    if fotos == 0:
        fotos = len(
            detail_soup.find_all(
                "img", src=re.compile(r"resizedimgs|akcdn|akamaized", re.I)
            )
        )
    result["fotos"] = fotos if fotos > 0 else np.nan

    below = detail_soup.find(attrs={"data-testid": "rp-card-belowPrice-txt"})
    result["preço abaixo do mercado"] = "Sim" if below else np.nan

    return result


# ---------------------------------------------------------------------------
# Salvar XLSX (parcial e final)
# ---------------------------------------------------------------------------


def _save_xlsx(data: list[dict], path: Path) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df["preço total"] = pd.to_numeric(df["preço total"], errors="coerce")
    df["metragem"] = pd.to_numeric(df["metragem"], errors="coerce")
    df["R$/m2"] = np.where(
        df["metragem"] > 0, df["preço total"] / df["metragem"], np.nan
    )
    df["R$/m2"] = df["R$/m2"].replace([np.inf, -np.inf], np.nan)
    df = df.reindex(columns=COLUNAS_FINAIS)
    df.to_excel(path, index=False, engine="openpyxl")
    return df


# ---------------------------------------------------------------------------
# Scrape principal
# ---------------------------------------------------------------------------


def _extract_links_from_page(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.find_all("li", {"data-cy": "rp-property-cd"})
    links: list[str] = []
    for card in cards:
        link_tag = card.find("a", href=True)
        if not link_tag:
            continue
        href = link_tag.get("href") or ""
        if href and not href.startswith("http"):
            href = "https://www.zapimoveis.com.br" + href
        if href and "/imovel/" in href:
            links.append(href)
    return links


def scrape(
    *,
    max_detalhes: int,
    max_paginas: int,
    proxy: str | None = None,
    n8n_json: bool = False,
    n8n_webhook: str | None = None,
) -> None:
    all_links: dict[str, str] = {}  # link -> segmento que o encontrou
    all_data: list[dict] = []

    session = _create_session(proxy=proxy)
    if not n8n_json and not n8n_webhook:
        print("Sessão curl_cffi criada (impersonate=chrome)")
        if proxy:
            print(f"  Proxy: {proxy}")

    segmentos = [
        (q, pmin, pmax)
        for q in QUARTOS
        for pmin, pmax in FAIXAS_PRECO
    ]
    total_seg = len(segmentos)
    avg_delay_s = (_DELAY_MIN + _DELAY_MAX) / 2
    http_por_req_s = 1.0

    # ================================================================
    # ETAPA 1 — coletar links de todos os segmentos
    # ================================================================
    if not n8n_json and not n8n_webhook:
        tqdm.write(f"\n{'='*60}")
        tqdm.write(f"ETAPA 1: Coletando links ({total_seg} segmentos)")
        tqdm.write(
            f"Estimativa superior (pior caso: {total_seg}×{max_paginas} págs): "
            f"~{total_seg * max_paginas * (avg_delay_s + http_por_req_s) / 60:.0f} min"
        )
        tqdm.write(f"{'='*60}")

    pbar_seg = tqdm(
        enumerate(segmentos, 1),
        total=total_seg,
        desc="Etapa 1: segmentos",
        unit="seg",
        dynamic_ncols=True,
        disable=(n8n_json or bool(n8n_webhook)),
    )
    for seg_idx, (quartos, preco_min, preco_max) in pbar_seg:
        label = _segment_label(quartos, preco_min, preco_max)
        if not n8n_json and not n8n_webhook:
            pbar_seg.set_postfix_str(label[:40], refresh=True)

        links_antes = len(all_links)
        paginas_vazias = 0

        pbar_pag = tqdm(
            range(1, max_paginas + 1),
            desc="  páginas",
            leave=False,
            total=max_paginas,
            unit="pág",
            dynamic_ncols=True,
            disable=(n8n_json or bool(n8n_webhook)),
        )
        for pag in pbar_pag:
            url = _build_search_url(quartos, preco_min, preco_max, pag)

            if pag > 1:
                time.sleep(random.uniform(_DELAY_MIN, _DELAY_MAX))

            html = _fetch(session, url, listing_page=True)
            if html is None:
                if not n8n_json and not n8n_webhook:
                    tqdm.write(f"    Pagina {pag}: sem HTML. Parando segmento.")
                break

            page_links = _extract_links_from_page(html)
            if not page_links:
                paginas_vazias += 1
                if paginas_vazias >= 2:
                    if not n8n_json and not n8n_webhook:
                        tqdm.write(
                            f"    Pagina {pag}: sem cards (2 vazias seguidas). Proximo segmento."
                        )
                    break
                if not n8n_json and not n8n_webhook:
                    tqdm.write(f"    Pagina {pag}: sem cards. Tentando mais uma...")
                continue

            paginas_vazias = 0
            novos = 0
            for link in page_links:
                if link not in all_links:
                    all_links[link] = label
                    novos += 1

            if not n8n_json and not n8n_webhook:
                pbar_pag.set_postfix_str(
                    f"cards={len(page_links)} +{novos} total={len(all_links)}",
                    refresh=True,
                )

            if novos == 0:
                if not n8n_json and not n8n_webhook:
                    tqdm.write(f"    Nenhum link novo. Proximo segmento.")
                break

        novos_seg = len(all_links) - links_antes
        if not n8n_json and not n8n_webhook:
            tqdm.write(f"  => {label}: +{novos_seg} links (acumulado: {len(all_links)})")

    if not n8n_json and not n8n_webhook:
        tqdm.write(f"\n  Total de links unicos: {len(all_links)}")

    # ================================================================
    # ETAPA 2 — buscar detalhes de cada imovel
    # ================================================================
    links_list = list(all_links.keys())
    if max_detalhes > 0:
        links_list = links_list[:max_detalhes]

    if not n8n_json and not n8n_webhook:
        tqdm.write(f"\n{'='*60}")
        tqdm.write(f"ETAPA 2: Detalhes ({len(links_list)} imóveis)")
        s_por_imovel = avg_delay_s + http_por_req_s
        tqdm.write(
            f"Estimativa inicial: ~{len(links_list) * s_por_imovel / 60:.1f} min "
            f"({len(links_list)} × ~{s_por_imovel:.1f}s médio; barra abaixo refina o ETA)"
        )
        tqdm.write(f"{'='*60}")

    pbar_det = tqdm(
        enumerate(links_list),
        total=len(links_list),
        desc="Etapa 2: detalhes",
        unit="imóvel",
        dynamic_ncols=True,
        disable=(n8n_json or bool(n8n_webhook)),
    )

    for i, link in pbar_det:
        if not n8n_json and not n8n_webhook:
            link_pv = (link[:50] + "…") if len(link) > 50 else link
            pbar_det.set_postfix_str(link_pv, refresh=False)

        if i > 0:
            time.sleep(random.uniform(_DELAY_MIN, _DELAY_MAX))

        html = _fetch(session, link)
        if html is None:
            if not n8n_json and not n8n_webhook:
                tqdm.write("    Bloqueado, pulando...")
            continue

        detail_soup = BeautifulSoup(html, "html.parser")
        data = extract_all_from_detail(detail_soup)
        data["link-href"] = link
        data["segmento"] = all_links[link]
        data["web-scraper-order"] = f"scraped-{int(time.time())}-{i}"
        data["web-scraper-start-url"] = START_URL_BASE
        
        # Converte np.nan para None antes de adicionar na lista para garantir JSON válido
        for key, value in data.items():
            if pd.isna(value):
                data[key] = None
                
        all_data.append(data)

        if not n8n_json and not n8n_webhook:
            titulo = (data.get("título") or "sem título")[:55]
            pbar_det.set_postfix_str(titulo, refresh=True)

            if len(all_data) % SAVE_INTERVAL == 0:
                _save_xlsx(all_data, OUTPUT_XLSX)
                tqdm.write(f"    [salvo parcial: {len(all_data)} imóveis]")

    # ================================================================
    # ETAPA 3 — salvar XLSX final e/ou Output/Envio Webhook
    # ================================================================
    if all_data:
        _save_xlsx(all_data, OUTPUT_XLSX)

    if n8n_webhook:
        # Envia os dados coletados diretamente para o Webhook do n8n via método POST
        print(f"Enviando dados para o Webhook: {n8n_webhook}")
        try:
            resp = session.post(n8n_webhook, json=all_data)
            print(f"Status Webhook n8n: HTTP {resp.status_code}")
        except Exception as e:
            print(f"Erro ao enviar para o Webhook: {e}")
        return

    if n8n_json:
        # Se rodar com --n8n-json, imprime apenas o array JSON no terminal para o n8n capturar.
        print(json.dumps(all_data, ensure_ascii=False))
        return

    print(f"\n{'='*60}")
    print(f"ETAPA 3: Salvando XLSX final")
    print(f"{'='*60}")

    if not all_data:
        print("Nenhum dado coletado.")
        return

    df = pd.DataFrame(all_data) # Criado só pro resumo final
    print(f"Arquivo: {OUTPUT_XLSX} ({len(df)} linhas)")
    print(f"\nResumo por segmento:")
    if "segmento" in df.columns:
        print(df["segmento"].value_counts().to_string())
    print(f"\n{df.head(10).to_string()}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Scraper Zap venda RJ — todos os imoveis, segmentado por quartos e preco"
    )
    p.add_argument(
        "--max-imoveis", type=int, default=0,
        help="Limite de imoveis para buscar detalhes (0 = sem limite)",
    )
    p.add_argument(
        "--max-paginas", type=int, default=MAX_PAGINAS_POR_SEGMENTO,
        help=(
            f"Max paginas por segmento (default: {MAX_PAGINAS_POR_SEGMENTO}; "
            "o site costuma devolver 404 depois da ~50ª)"
        ),
    )
    p.add_argument(
        "--proxy", type=str, default=None,
        help="Proxy (ex: socks5://127.0.0.1:9050, http://user:pass@host:port)",
    )
    p.add_argument(
        "--n8n-json", action="store_true",
        help="Retorna o output final exclusivamente em formato JSON para captura no terminal do n8n",
    )
    # Novo parâmetro para o cenário 2 (GitHub Actions enviando pro n8n)
    p.add_argument(
        "--n8n-webhook", type=str, default=None,
        help="URL do nó Webhook do seu n8n. Se fornecido, faz um POST com o JSON dos imóveis.",
    )
    args = p.parse_args()
    scrape(
        max_detalhes=max(0, args.max_imoveis),
        max_paginas=max(1, args.max_paginas),
        proxy=args.proxy,
        n8n_json=args.n8n_json,
        n8n_webhook=args.n8n_webhook,
    )
