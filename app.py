import os
from decimal import Decimal, InvalidOperation
from functools import wraps

import psycopg
from flask import Flask, render_template, request, redirect, url_for, flash, session

from db import get_connection

app = Flask(__name__, template_folder="templates")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "interlaser_secret_2026")


def format_int(value):
    try:
        return f"{round(float(value or 0)):.0f}"
    except (TypeError, ValueError):
        return "0"


app.jinja_env.filters["format_int"] = format_int
app.jinja_env.filters["fmt_num"] = format_int


def get_allowed_users():
    raw = os.getenv("APP_USERS", "admin:123456,estoque:123456,interlaser:123456")
    users = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        user, pwd = item.split(":", 1)
        users[user.strip()] = pwd.strip()
    return users


def parse_float(value, default=0.0):
    """
    Aceita entradas como:
    10
    10,5
    10.5
    1.234,56
    1,234.56
    """
    if value is None:
        return default

    txt = str(value).strip()
    if not txt:
        return default

    txt = txt.replace(" ", "")

    if "," in txt and "." in txt:
        # O último separador tende a ser o decimal.
        if txt.rfind(",") > txt.rfind("."):
            txt = txt.replace(".", "").replace(",", ".")
        else:
            txt = txt.replace(",", "")
    elif "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    else:
        # Só ponto: decide se é decimal ou milhar.
        if txt.count(".") > 1:
            parts = txt.split(".")
            txt = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(Decimal(txt))
    except (InvalidOperation, ValueError, TypeError):
        return default


def handle_db_error(conn, route_name, user_message, **kwargs):
    if conn:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
    flash(user_message, "danger")
    return redirect(url_for(route_name, **kwargs))


@app.context_processor
def inject_now_user():
    return {"usuario_logado": session.get("usuario", "")}


@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = (request.form.get("usuario") or "").strip()
        senha = (request.form.get("senha") or "").strip()

        if not usuario or not senha:
            flash("Informe usuário e senha.", "danger")
            return render_template("login.html")

        users = get_allowed_users()
        senha_esperada = users.get(usuario)

        if senha_esperada is None or senha != senha_esperada:
            flash("Usuário ou senha inválidos.", "danger")
            return render_template("login.html")

        session["usuario"] = usuario
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sessão encerrada.", "success")
    return redirect(url_for("login"))


def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            return redirect(url_for("login"))
        return func(*args, **kwargs)

    return wrapper


def recalcular_total(b1, b2):
    return float(b1 or 0) + float(b2 or 0)


def atualizar_saldos_produto(cur, produto_id, saldo_b1, saldo_b2):
    estoque_atual = recalcular_total(saldo_b1, saldo_b2)
    cur.execute(
        """
        UPDATE produtos
        SET saldo_b1 = %s,
            saldo_b2 = %s,
            estoque_atual = %s
        WHERE id = %s
        """,
        (saldo_b1, saldo_b2, estoque_atual, produto_id),
    )



def registrar_movimentacao(cur, produto_id, tipo, quantidade, barracao, usuario, origem, observacao=""):
    cur.execute(
        """
        INSERT INTO movimentacoes (
            produto_id, tipo, quantidade, barracao, usuario_mov, origem, observacao
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (produto_id, tipo, quantidade, barracao, usuario, origem, observacao),
    )


@app.route("/dashboard")
@login_required
def dashboard():
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM produtos")
        total_produtos = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(COALESCE(saldo_b1, 0) + COALESCE(saldo_b2, 0)), 0) FROM produtos")
        total_estoque = cur.fetchone()[0] or 0

        cur.execute(
            """
            SELECT COUNT(*)
            FROM produtos
            WHERE COALESCE(saldo_b1, 0) + COALESCE(saldo_b2, 0) < COALESCE(estoque_minimo, 0)
            """
        )
        abaixo_minimo = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM solicitacoes_saida WHERE situacao IN ('PENDENTE', 'PARCIAL')")
        pendentes = cur.fetchone()[0]

        cur.execute(
            """
            SELECT
                m.id,
                p.codigo,
                p.descricao,
                m.tipo,
                m.quantidade,
                m.barracao,
                m.usuario_mov,
                m.origem,
                m.observacao,
                TO_CHAR(m.data_mov, 'DD/MM/YYYY HH24:MI')
            FROM movimentacoes m
            JOIN produtos p ON p.id = m.produto_id
            ORDER BY m.data_mov DESC, m.id DESC
            LIMIT 10
            """
        )
        movimentacoes = cur.fetchall()

        return render_template(
            "dashboard.html",
            total_produtos=total_produtos,
            total_estoque=total_estoque,
            abaixo_minimo=abaixo_minimo,
            pendentes=pendentes,
            movimentacoes=movimentacoes,
        )
    except psycopg.Error:
        flash("Não foi possível carregar o dashboard agora. Verifique a conexão com o banco.", "danger")
        return render_template(
            "dashboard.html",
            total_produtos=0,
            total_estoque=0,
            abaixo_minimo=0,
            pendentes=0,
            movimentacoes=[],
        )
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/produtos")
@login_required
def produtos():
    busca = (request.args.get("busca") or "").strip()

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT
                id,
                codigo,
                descricao,
                COALESCE(unidade, '') AS unidade,
                COALESCE(estoque_atual, 0) AS estoque_atual,
                COALESCE(estoque_minimo, 0) AS estoque_minimo,
                COALESCE(grupo, '') AS grupo,
                COALESCE(barracao, '') AS barracao,
                COALESCE(saldo_b1, 0) AS saldo_b1,
                COALESCE(saldo_b2, 0) AS saldo_b2,
                COALESCE(saldo_b1, 0) + COALESCE(saldo_b2, 0) AS saldo_total
            FROM produtos
        """
        params = []

        if busca:
            sql += " WHERE codigo ILIKE %s OR descricao ILIKE %s OR COALESCE(grupo, '') ILIKE %s"
            params.extend([f"%{busca}%", f"%{busca}%", f"%{busca}%"])

        sql += " ORDER BY descricao"
        cur.execute(sql, tuple(params))
        lista_produtos = cur.fetchall()

        return render_template("produtos.html", produtos=lista_produtos, busca=busca)
    except psycopg.Error:
        flash("Não foi possível carregar os produtos agora.", "danger")
        return render_template("produtos.html", produtos=[], busca=busca)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/produto/novo", methods=["GET", "POST"])
@login_required
def novo_produto():
    if request.method == "POST":
        codigo = (request.form.get("codigo") or "").strip().upper()
        descricao = (request.form.get("descricao") or "").strip()
        unidade = (request.form.get("unidade") or "UN").strip().upper()
        estoque_minimo = parse_float(request.form.get("estoque_minimo"), 0)
        grupo = (request.form.get("grupo") or "").strip()
        barracao = (request.form.get("barracao") or "").strip().upper()
        saldo_b1 = parse_float(request.form.get("saldo_b1"), 0)
        saldo_b2 = parse_float(request.form.get("saldo_b2"), 0)
        estoque_atual = saldo_b1 + saldo_b2

        if not codigo or not descricao:
            flash("Preencha código e descrição.", "danger")
            return render_template("novo_produto.html")

        conn = None
        cur = None
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id FROM produtos WHERE codigo = %s", (codigo,))
            existente = cur.fetchone()
            if existente:
                flash("Já existe um produto com esse código.", "danger")
                return render_template("novo_produto.html")

            cur.execute(
                """
                INSERT INTO produtos (
                    codigo, descricao, unidade, estoque_atual, estoque_minimo,
                    grupo, barracao, saldo_b1, saldo_b2
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (codigo, descricao, unidade, estoque_atual, estoque_minimo, grupo, barracao, saldo_b1, saldo_b2),
            )
            conn.commit()
            flash("Produto cadastrado com sucesso.", "success")
            return redirect(url_for("produtos"))
        except psycopg.Error:
            return handle_db_error(conn, "novo_produto", "Não foi possível cadastrar o produto agora.")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    return render_template("novo_produto.html")


@app.route("/produto/<int:id>/editar", methods=["GET", "POST"])
@login_required
def editar_produto(id):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if request.method == "POST":
            codigo = (request.form.get("codigo") or "").strip().upper()
            descricao = (request.form.get("descricao") or "").strip()
            unidade = (request.form.get("unidade") or "UN").strip().upper()
            estoque_minimo = parse_float(request.form.get("estoque_minimo"), 0)
            grupo = (request.form.get("grupo") or "").strip()
            barracao = (request.form.get("barracao") or "").strip().upper()
            saldo_b1 = parse_float(request.form.get("saldo_b1"), 0)
            saldo_b2 = parse_float(request.form.get("saldo_b2"), 0)
            estoque_atual = saldo_b1 + saldo_b2

            if not codigo or not descricao:
                flash("Preencha código e descrição.", "danger")
            else:
                cur.execute("SELECT id FROM produtos WHERE codigo = %s AND id <> %s", (codigo, id))
                existente = cur.fetchone()
                if existente:
                    flash("Já existe outro produto com esse código.", "danger")
                else:
                    cur.execute(
                        """
                        UPDATE produtos
                        SET codigo = %s,
                            descricao = %s,
                            unidade = %s,
                            estoque_atual = %s,
                            estoque_minimo = %s,
                            grupo = %s,
                            barracao = %s,
                            saldo_b1 = %s,
                            saldo_b2 = %s
                        WHERE id = %s
                        """,
                        (codigo, descricao, unidade, estoque_atual, estoque_minimo, grupo, barracao, saldo_b1, saldo_b2, id),
                    )
                    conn.commit()
                    flash("Produto atualizado com sucesso.", "success")
                    return redirect(url_for("produtos"))

        cur.execute(
            """
            SELECT
                id,
                codigo,
                descricao,
                COALESCE(unidade, 'UN'),
                COALESCE(estoque_minimo, 0),
                COALESCE(grupo, ''),
                COALESCE(barracao, ''),
                COALESCE(saldo_b1, 0),
                COALESCE(saldo_b2, 0)
            FROM produtos
            WHERE id = %s
            """,
            (id,),
        )
        produto = cur.fetchone()

        if not produto:
            flash("Produto não encontrado.", "danger")
            return redirect(url_for("produtos"))

        return render_template("editar_produto.html", produto=produto)
    except psycopg.Error:
        return handle_db_error(conn, "produtos", "Não foi possível carregar ou atualizar o produto agora.")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/movimentacoes")
@login_required
def movimentacoes():
    busca = (request.args.get("busca") or "").strip()
    tipo = (request.args.get("tipo") or "").strip().upper()

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT
                m.id,
                p.codigo,
                p.descricao,
                m.tipo,
                m.quantidade,
                m.barracao,
                COALESCE(m.usuario_mov, '') AS usuario_mov,
                COALESCE(m.origem, '') AS origem,
                COALESCE(m.observacao, '') AS observacao,
                TO_CHAR(m.data_mov, 'DD/MM/YYYY HH24:MI') AS data_mov
            FROM movimentacoes m
            JOIN produtos p ON p.id = m.produto_id
            WHERE 1 = 1
        """
        params = []

        if busca:
            sql += " AND (p.codigo ILIKE %s OR p.descricao ILIKE %s OR COALESCE(m.usuario_mov, '') ILIKE %s)"
            params.extend([f"%{busca}%", f"%{busca}%", f"%{busca}%"])

        if tipo:
            sql += " AND m.tipo = %s"
            params.append(tipo)

        sql += " ORDER BY m.data_mov DESC, m.id DESC"

        cur.execute(sql, tuple(params))
        dados = cur.fetchall()

        return render_template("movimentacoes_lista.html", movimentacoes=dados, busca=busca, filtro_tipo=tipo)
    except psycopg.Error:
        flash("Não foi possível carregar as movimentações agora.", "danger")
        return render_template("movimentacoes_lista.html", movimentacoes=[], busca=busca, filtro_tipo=tipo)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/movimentacao/nova", methods=["GET", "POST"])
@login_required
def nova_movimentacao():
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if request.method == "POST":
            produto_id = request.form.get("produto")
            tipo = (request.form.get("tipo") or "").strip().upper()
            quantidade = parse_float(request.form.get("quantidade"), 0)
            barracao = (request.form.get("barracao") or "").strip().upper()
            obs = (request.form.get("observacao") or "").strip()
            usuario = session["usuario"]

            if not produto_id:
                flash("Selecione um produto.", "danger")
                cur.execute("SELECT id, codigo, descricao FROM produtos ORDER BY descricao")
                produtos = cur.fetchall()
                return render_template("movimentacao.html", produtos=produtos)

            if quantidade <= 0:
                flash("Informe uma quantidade válida.", "danger")
                cur.execute("SELECT id, codigo, descricao FROM produtos ORDER BY descricao")
                produtos = cur.fetchall()
                return render_template("movimentacao.html", produtos=produtos)

            cur.execute(
                "SELECT COALESCE(saldo_b1, 0), COALESCE(saldo_b2, 0) FROM produtos WHERE id = %s",
                (produto_id,),
            )
            saldos = cur.fetchone()

            if not saldos:
                flash("Produto não encontrado.", "danger")
                return redirect(url_for("nova_movimentacao"))

            b1, b2 = map(float, saldos)

            if tipo == "ENTRADA":
                if barracao == "B1":
                    b1 += quantidade
                elif barracao == "B2":
                    b2 += quantidade
                else:
                    flash("Barracão inválido.", "danger")
                    return redirect(url_for("nova_movimentacao"))

            elif tipo == "SAIDA":
                if barracao == "B1":
                    if quantidade > b1:
                        flash("Estoque insuficiente no B1.", "danger")
                        return redirect(url_for("nova_movimentacao"))
                    b1 -= quantidade
                elif barracao == "B2":
                    if quantidade > b2:
                        flash("Estoque insuficiente no B2.", "danger")
                        return redirect(url_for("nova_movimentacao"))
                    b2 -= quantidade
                else:
                    flash("Barracão inválido.", "danger")
                    return redirect(url_for("nova_movimentacao"))

            elif tipo == "AJUSTE":
                if barracao == "B1":
                    b1 = quantidade
                elif barracao == "B2":
                    b2 = quantidade
                else:
                    flash("Barracão inválido.", "danger")
                    return redirect(url_for("nova_movimentacao"))
            else:
                flash("Tipo de movimentação inválido.", "danger")
                return redirect(url_for("nova_movimentacao"))

            atualizar_saldos_produto(cur, produto_id, b1, b2)
            registrar_movimentacao(cur, produto_id, tipo, quantidade, barracao, usuario, "MANUAL", obs)
            conn.commit()

            flash("Movimentação realizada com sucesso.", "success")
            return redirect(url_for("movimentacoes"))

        cur.execute("SELECT id, codigo, descricao FROM produtos ORDER BY descricao")
        produtos = cur.fetchall()
        return render_template("movimentacao.html", produtos=produtos)
    except psycopg.Error:
        return handle_db_error(conn, "nova_movimentacao", "Não foi possível registrar a movimentação agora.")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/solicitacoes")
@login_required
def solicitacoes():
    busca = (request.args.get("busca") or "").strip()
    situacao = (request.args.get("situacao") or "").strip().upper()

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT
                s.id,
                s.data_solicitacao,
                p.codigo,
                p.descricao,
                s.quantidade,
                s.qtd_atendida,
                s.situacao,
                s.barracao,
                s.usuario_solicitacao,
                s.usuario_atendimento,
                s.motivo_recusa,
                s.tempo_finalizado
            FROM solicitacoes_saida s
            JOIN produtos p ON p.id = s.produto_id
            WHERE 1=1
        """
        params = []

        if busca:
            sql += " AND (p.codigo ILIKE %s OR p.descricao ILIKE %s OR COALESCE(s.usuario_solicitacao, '') ILIKE %s)"
            params.extend([f"%{busca}%", f"%{busca}%", f"%{busca}%"])

        if situacao:
            sql += " AND s.situacao = %s"
            params.append(situacao)

        sql += " ORDER BY s.id DESC"
        cur.execute(sql, tuple(params))
        dados = cur.fetchall()

        return render_template(
            "solicitacoes.html",
            solicitacoes=dados,
            busca=busca,
            filtro_situacao=situacao,
        )
    except psycopg.Error:
        flash("Não foi possível carregar as solicitações agora.", "danger")
        return render_template("solicitacoes.html", solicitacoes=[], busca=busca, filtro_situacao=situacao)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/solicitacao/nova", methods=["GET", "POST"])
@login_required
def nova_solicitacao():
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        if request.method == "POST":
            produto_id = request.form.get("produto")
            quantidade = parse_float(request.form.get("quantidade"), 0)
            usuario = session["usuario"]

            if not produto_id or quantidade <= 0:
                flash("Preencha produto e quantidade corretamente.", "danger")
                cur.execute("SELECT id, codigo, descricao FROM produtos ORDER BY descricao")
                produtos = cur.fetchall()
                return render_template("nova_solicitacao.html", produtos=produtos)

            cur.execute(
                """
                INSERT INTO solicitacoes_saida (
                    nome, produto_id, quantidade, qtd_atendida, situacao, barracao, usuario_solicitacao
                )
                VALUES (%s, %s, %s, 0, 'PENDENTE', NULL, %s)
                """,
                (usuario, produto_id, quantidade, usuario),
            )

            conn.commit()
            flash("Solicitação criada.", "success")
            return redirect(url_for("solicitacoes"))

        cur.execute("SELECT id, codigo, descricao FROM produtos ORDER BY descricao")
        produtos = cur.fetchall()
        return render_template("nova_solicitacao.html", produtos=produtos)
    except psycopg.Error:
        return handle_db_error(conn, "solicitacoes", "Não foi possível criar a solicitação agora.")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/solicitacao/<int:id>/atender", methods=["GET", "POST"])
@login_required
def atender_solicitacao(id):
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                s.id,
                s.produto_id,
                s.quantidade,
                COALESCE(s.qtd_atendida, 0),
                s.situacao,
                s.barracao,
                p.codigo,
                p.descricao,
                COALESCE(p.saldo_b1, 0),
                COALESCE(p.saldo_b2, 0),
                s.data_solicitacao,
                s.usuario_solicitacao,
                COALESCE(s.motivo_recusa, '')
            FROM solicitacoes_saida s
            JOIN produtos p ON p.id = s.produto_id
            WHERE s.id = %s
            """,
            (id,),
        )
        sol = cur.fetchone()

        if not sol:
            flash("Solicitação não encontrada.", "danger")
            return redirect(url_for("solicitacoes"))

        if sol[4] in ("ATENDIDA", "RECUSADA"):
            flash("Essa solicitação já foi finalizada.", "warning")
            return redirect(url_for("solicitacoes"))

        if request.method == "POST":
            acao = (request.form.get("acao") or "ATENDER").strip().upper()
            usuario = session["usuario"]

            if acao == "RECUSAR":
                motivo_recusa = (request.form.get("motivo_recusa") or "").strip()
                if not motivo_recusa:
                    flash("Informe o motivo da recusa.", "danger")
                    return redirect(url_for("atender_solicitacao", id=id))

                cur.execute(
                    """
                    UPDATE solicitacoes_saida
                    SET situacao = 'RECUSADA',
                        usuario_atendimento = %s,
                        motivo_recusa = %s,
                        tempo_finalizado = NOW()
                    WHERE id = %s
                    """,
                    (usuario, motivo_recusa, id),
                )
                conn.commit()
                flash("Solicitação recusada.", "warning")
                return redirect(url_for("solicitacoes"))

            qtd = parse_float(request.form.get("qtd"), 0)
            barracao = (request.form.get("barracao") or "").strip().upper()

            if qtd <= 0:
                flash("Informe uma quantidade válida para atendimento.", "danger")
                return redirect(url_for("atender_solicitacao", id=id))

            if barracao not in ("B1", "B2"):
                flash("Selecione o barracão de atendimento.", "danger")
                return redirect(url_for("atender_solicitacao", id=id))

            quantidade_solicitada = float(sol[2] or 0)
            qtd_atendida_atual = float(sol[3] or 0)
            restante = quantidade_solicitada - qtd_atendida_atual

            if qtd > restante:
                flash("A quantidade informada é maior que o saldo pendente da solicitação.", "danger")
                return redirect(url_for("atender_solicitacao", id=id))

            b1 = float(sol[8] or 0)
            b2 = float(sol[9] or 0)

            if barracao == "B1":
                if qtd > b1:
                    flash("Estoque insuficiente no B1.", "danger")
                    return redirect(url_for("atender_solicitacao", id=id))
                b1 -= qtd
            else:
                if qtd > b2:
                    flash("Estoque insuficiente no B2.", "danger")
                    return redirect(url_for("atender_solicitacao", id=id))
                b2 -= qtd

            novo_qtd_atendida = qtd_atendida_atual + qtd
            nova_situacao = "ATENDIDA" if novo_qtd_atendida >= quantidade_solicitada else "PARCIAL"

            atualizar_saldos_produto(cur, sol[1], b1, b2)
            cur.execute(
                """
                UPDATE solicitacoes_saida
                SET qtd_atendida = %s,
                    situacao = %s,
                    barracao = %s,
                    usuario_atendimento = %s,
                    tempo_finalizado = CASE WHEN %s = 'ATENDIDA' THEN NOW() ELSE tempo_finalizado END
                WHERE id = %s
                """,
                (novo_qtd_atendida, nova_situacao, barracao, usuario, nova_situacao, id),
            )

            registrar_movimentacao(cur, sol[1], "SAIDA", qtd, barracao, usuario, "SOLICITACAO", f"Atendimento da solicitação #{id}")
            conn.commit()
            flash("Solicitação atualizada com sucesso.", "success")
            return redirect(url_for("solicitacoes"))

        return render_template("atender_solicitacao.html", sol=sol)
    except psycopg.Error:
        return handle_db_error(conn, "solicitacoes", "Não foi possível processar a solicitação agora.")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/relatorios")
@login_required
def relatorios():
    busca = (request.args.get("busca") or "").strip()
    tipo = (request.args.get("tipo") or "todos").strip()

    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                codigo,
                descricao,
                COALESCE(saldo_b1, 0) AS saldo_b1,
                COALESCE(saldo_b2, 0) AS saldo_b2,
                COALESCE(estoque_minimo, 0) AS estoque_minimo,
                COALESCE(saldo_b1, 0) + COALESCE(saldo_b2, 0) AS saldo_total
            FROM produtos
            ORDER BY descricao
            """
        )
        dados = cur.fetchall()

        itens = []
        for item in dados:
            codigo, descricao, saldo_b1, saldo_b2, estoque_minimo, saldo_total = item
            texto = f"{codigo} {descricao}".lower()
            if busca and busca.lower() not in texto:
                continue
            if tipo == "com_saldo" and saldo_total <= 0:
                continue
            if tipo == "abaixo_minimo" and saldo_total >= estoque_minimo:
                continue
            itens.append(item)

        return render_template("relatorios.html", itens=itens, busca=busca, tipo=tipo)
    except psycopg.Error:
        flash("Não foi possível gerar o relatório agora.", "danger")
        return render_template("relatorios.html", itens=[], busca=busca, tipo=tipo)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("FLASK_PORT", "5000")))
    app.run(host=host, port=port, debug=debug_mode)
