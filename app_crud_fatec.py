# Instale as dependências fora do script, por exemplo:
#   pip install psycopg2-binary python-dotenv

from typing import Optional, Iterator # Para Anotações de Tipo (avançado)
import psycopg2 as pg # Biblioteca para PostgreSQL
from psycopg2.extensions import connection # Tipo de conexão do psycopg2
from psycopg2.extensions import connection as PgConnection # Tipo de conexão do psycopg2
from psycopg2 import pool # Para Pool de Conexões
import os # Para ler variáveis de ambiente
import sys # Para sys.exit()
import getpass # Importa para esconder a senha no terminal
from dotenv import load_dotenv # Para carregar variáveis do .env
import json # Para manipulação de JSON
import requests # Para requisições HTTP (importação de dados da web)
import zipfile  # Para manipulação de arquivos ZIP
from datetime import datetime # Para manipulação de datas
from contextlib import contextmanager # Para gerenciar contexto de conexão
from colorama import init, Fore, Style # Para cores no terminal

# Inicializa Colorama (Windows)
init(autoreset=True)

load_dotenv()  # Carrega as variáveis do arquivo .env

# --- Configuração do Banco de Dados ---
# ATENÇÃO: A senha será lida de uma VARIÁVEL DE AMBIENTE para segurança!
DB_CONFIG = {
    "database": os.getenv("DB_NAME", "db_prova_crud"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD"),  # <==== Lendo a Senha da Variavel de Ambiente
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5432")
}

def conectar_db() -> Optional[connection]:
    """Tenta estabelecer a conexão com o PostgreSQL, lendo a senha do ambiente."""
    # A verificação da senha é feita aqui:
    if not DB_CONFIG['password']:
        print("\n❌ ERRO CRÍTICO: Variável de ambiente 'PG_PASSWORD' não configurada.")
        print("Defina a variável no seu terminal antes de rodar o programa ou crie um arquivo .env com PG_PASSWORD.")
        pause()
        sys.exit(1)
        
    try:
        con = pg.connect(**DB_CONFIG)
        print("✅ Conexão com o banco de dados estabelecida com sucesso!")
        return con
    except Exception as erro:
        print(f"\n❌ ERRO CRÍTICO: Não foi possível conectar ao banco de dados. Detalhes: {erro}")
        sys.exit(1)
        return None

#--- Pool de Conexões ---
# Utilizando pool de conexões para melhor performance em aplicações maiores
# Exemplo básico de pool de conexões
class DatabasePool:
    _pool = None

    @classmethod
    def get_pool(cls, minconn=1, maxconn=10):
        if cls._pool is None:
            if not DB_CONFIG['password']:
                print("\n❌ ERRO CRÍTICO: Variável de ambiente 'PG_PASSWORD' não configurada.")
                print("Defina a variável no seu terminal ou crie um arquivo .env com PG_PASSWORD.")
                pause()
                sys.exit(1)
            print("Inicializando pool de conexões...")
            cls._pool = pool.SimpleConnectionPool(minconn, maxconn, **DB_CONFIG)
            print("✅ Pool de conexões pronto.")
        return cls._pool

    @classmethod
    def get_connection(cls):
        return cls.get_pool().getconn()

    @classmethod
    def return_connection(cls, conn):
        cls.get_pool().putconn(conn)
        

    @classmethod
    def close_all(cls):
        if cls._pool:
            cls._pool.closeall()
            print("\nTodas as conexões do pool foram encerradas.")

@contextmanager
def get_db_connection() -> Iterator[PgConnection]:
    """Gerenciador de contexto para obter e devolver uma conexão do pool."""
    conn = DatabasePool.get_connection()
    try:
        yield conn
    finally:
        DatabasePool.return_connection(conn)

def log_evento(level: str, message: str):
    """Registra um evento (log) na tabela de logs do banco de dados."""
    # Não queremos que um erro de log quebre a aplicação principal.
    try:
        with get_db_connection() as con:
            with con.cursor() as cur:
                cur.execute(
                    "INSERT INTO logs (level, message) VALUES (%s, %s)",
                    (level.upper(), message)
                )
            con.commit()
    except Exception as e:
        # Se o logging no banco falhar, imprime no console como fallback.
        print(f"CRITICAL LOGGING ERROR: {e}")

#--- Criação e Inicialização das Tabelas ---
# As tabelas serão criadas se não existirem
def criar_tabelas():
    """Cria e inicializa as tabelas necessárias para o projeto."""
    
    # As instruções DDL (Data Definition Language)
    sql_ddl = """
    -- 1. Tabela Usuários (Requisito de Login)
    CREATE TABLE IF NOT EXISTS usuarios (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        senha VARCHAR(100) NOT NULL
    );

    -- 2. Tabela Clientes (Principal CRUD - Mínimo 3 operações)
    CREATE TABLE IF NOT EXISTS clientes (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        telefone VARCHAR(20)
    );
    
    -- 3. Tabela Pedidos (Terceira Tabela de Requisito)
    CREATE TABLE IF NOT EXISTS pedidos (
        id SERIAL PRIMARY KEY,
        cliente_id INTEGER REFERENCES clientes(id) ON DELETE CASCADE,
        data_pedido DATE NOT NULL DEFAULT CURRENT_DATE,
        item VARCHAR(100),
        valor DECIMAL(10, 2)
    );
    
    -- 4. Tabela Dados Importados (Requisito de Importação da Web)
    CREATE TABLE IF NOT EXISTS dados_importados (
        id SERIAL PRIMARY KEY,
        dado_json JSONB
        , status VARCHAR(20) NOT NULL DEFAULT 'NOVO' -- NOVO, PROCESSADO, ERRO
    );

    -- 5. Tabela de Logs para auditoria e debug
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        level VARCHAR(10) NOT NULL,
        message TEXT NOT NULL
    );
    """
    with get_db_connection() as con:
        try:
            with con.cursor() as cur:
                cur.execute(sql_ddl)
                
                # Inclui um usuário padrão para teste de login (admin/admin123)
                cur.execute(
                    "INSERT INTO usuarios (username, senha) VALUES (%s, %s) ON CONFLICT (username) DO NOTHING",
                    ('admin', 'admin123')
                )
                
                # --- Migração Simples: Garante que a coluna 'item' exista ---
                # Esta é uma forma de atualizar a tabela sem precisar recriá-la.
                cur.execute("""
                    ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS item VARCHAR(100);
                """)

                # Garante que a coluna 'status' exista na tabela de dados importados
                cur.execute("""
                    ALTER TABLE dados_importados ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'NOVO';
                """)
            con.commit()
            print("✅ Estrutura das tabelas criada/verificada e usuário padrão inserido.")
        except Exception as erro:
            print(f"❌ ERRO ao criar ou inicializar tabelas: {erro}")
            con.rollback() # Desfaz as mudanças em caso de erro

# =========================================================================
# FUNÇÕES DE INTERFACE DO USUÁRIO (UI 1 e UI 2) - Adicionadas
# =========================================================================

def verificar_credenciais(con, username, senha):
    """Verifica se o usuário e senha são válidos na tabela 'usuarios'."""
    sql = "SELECT username FROM usuarios WHERE username = %s AND senha = %s"
    try:
        cur = con.cursor()
        cur.execute(sql, (username, senha))
        usuario = cur.fetchone()
        return usuario is not None
    except Exception as erro:
        print(f"Erro ao verificar credenciais: {erro}")
        pause()
        return False

def ui_login(con):
    """Interface Gráfica 1: Login (Requisito obrigatório)."""
    print("\n" + Fore.CYAN + "="*40 + Style.RESET_ALL)
    print(Fore.CYAN + "        SISTEMA DE CADASTRO - LOGIN" + Style.RESET_ALL)
    print(Fore.CYAN + "="*40 + Style.RESET_ALL)
    tentativas = 0
    while tentativas < 3:
        username = input(Fore.YELLOW + "Usuário: " + Style.RESET_ALL)
        senha = getpass.getpass(Fore.YELLOW + "Senha: " + Style.RESET_ALL)
        if verificar_credenciais(con, username, senha):
            log_evento("INFO", f"Login bem-sucedido para o usuário '{username}'.")
            print("\n" + Fore.GREEN + "ACESSO CONCEDIDO. Bem-vindo(a)!" + Style.RESET_ALL)
            # Select nome do usuario logado para exibir
            print(Fore.GREEN + f"Usuário logado: {username}" + Style.RESET_ALL)

            pause()
            return True
        else:
            log_evento("WARNING", f"Tentativa de login falhou para o usuário '{username}'.")
            tentativas += 1
            print("\n" + Fore.RED + "Credenciais inválidas. Tente novamente." + Style.RESET_ALL)
            pause()
    log_evento("ERROR", "Número máximo de tentativas de login excedido.")
    print("\n" + Fore.RED + "Número máximo de tentativas excedido. Encerrando." + Style.RESET_ALL)
    pause()
    return False

def ui_sobre():
    """Interface Gráfica 3: Tela Sobre (Requisito obrigatório)."""
    print("\n" + Fore.MAGENTA + "="*40 + Style.RESET_ALL)
    print(Fore.MAGENTA + "             SOBRE O PROJETO" + Style.RESET_ALL)
    print(Fore.MAGENTA + "="*40 + Style.RESET_ALL)
    print(Fore.CYAN + "TEMA ESCOLHIDO: " + Style.RESET_ALL + "Gerenciamento de Cadastros Simples")
    print(Fore.CYAN + "OBJETIVO: " + Style.RESET_ALL + "Desenvolver um aplicativo CRUD completo.")
    print("\n" + Fore.YELLOW + "DESENVOLVEDOR:" + Style.RESET_ALL)
    print("  - " + Fore.GREEN + "Nome: " + Style.RESET_ALL + "Allandre Ramos Girodo")
    print("  - " + Fore.GREEN + "Matrícula: " + Style.RESET_ALL + "[2840482323046]")
    print("  - " + Fore.GREEN + "Curso: " + Style.RESET_ALL + "Análise e Desenvolvimento de Sistemas - Fatec Ribeirão Preto")
    print(Fore.MAGENTA + "="*40 + Style.RESET_ALL)
    
    # Verifica o sistema operacional
    if os.name == "nt":
        so_legivel = "Windows (NT)"
    elif os.name == "posix":
        so_legivel = "Linux / macOS (POSIX)"
    elif os.name == "java":
        so_legivel = "Java Virtual Machine (Jython)"
    else:
        so_legivel = "Sistema Desconhecido"

    print(Fore.GREEN + "INFORMAÇÕES DO SISTEMA:")
    print(Fore.GREEN + "Sistema identificado: " + Fore.WHITE + so_legivel)
    print(Fore.GREEN + "Usuário logado: " + Fore.WHITE + os.getlogin())
    print(Fore.GREEN + "Diretório atual: " + Fore.WHITE + os.getcwd() + Style.RESET_ALL)
    print(Fore.MAGENTA + "+"*40 + Style.RESET_ALL)
    print(Fore.MAGENTA + "Versão do Changelog: " + Style.RESET_ALL + "1.0.0")

def ui_menu_principal(con):
    """UI 2: Menu Principal."""
    while True:
        clear_screen()
        print("\n" + Fore.CYAN + "*"*40)
        print(Fore.CYAN + "        MENU PRINCIPAL DO SISTEMA")
        print("*"*40)
        print(Fore.YELLOW + "[1] " + Style.RESET_ALL + "Gerenciar " + Fore.GREEN + "CLIENTES" + Style.RESET_ALL + " (CRUD)")
        print(Fore.YELLOW + "[2] " + Style.RESET_ALL + "Gerenciar " + Fore.GREEN + "PEDIDOS" + Style.RESET_ALL + " (CRUD)")
        print(Fore.YELLOW + "[3] " + Style.RESET_ALL + "Funcionalidades Especiais " + Fore.BLUE + "(Importar/Exportar)")
        print(Fore.YELLOW + "[4] " + Style.RESET_ALL + "Sobre o Sistema")
        print(Fore.RED + "[0] " + Style.RESET_ALL + "Sair do Sistema")
        print(Fore.CYAN + "*"*40 + Style.RESET_ALL)
        try:
            opcao = int(input(Fore.MAGENTA + "Escolha a opção desejada: " + Style.RESET_ALL))
            if opcao == 1:
                ui_menu_clientes(con)
            elif opcao == 2:
                ui_menu_pedidos(con)
            elif opcao == 3:
                ui_menu_especial(con)
            elif opcao == 4:
                ui_sobre()
                pause()
            elif opcao == 0:
                print(Fore.GREEN + "Encerrando aplicação. 😄" + Style.RESET_ALL) # Sair do Sistema
                log_evento("INFO", "Aplicação encerrada pelo usuário.")
                break
            else:
                print(Fore.RED + "Opção inválida." + Style.RESET_ALL)
        except ValueError:
            print(Fore.RED + "Entrada inválida. Digite um número." + Style.RESET_ALL)

def ui_menu_clientes(con):
    """UI 4: Menu de Gerenciamento de Clientes."""
    while True:
        clear_screen()
        print("\n" + Fore.CYAN + "="*40)
        print("        MENU GERENCIAR CLIENTES")
        print("="*40)
        print(Fore.YELLOW + "[1] " + Style.RESET_ALL + "Listar Clientes " + Fore.GREEN + "(READ)")
        print(Fore.YELLOW + "[2] " + Style.RESET_ALL + "Cadastrar Novo " + Fore.GREEN + "(CREATE)")
        print(Fore.YELLOW + "[3] " + Style.RESET_ALL + "Editar Cliente " + Fore.GREEN + "(UPDATE)")
        print(Fore.YELLOW + "[4] " + Style.RESET_ALL + "Excluir Cliente " + Fore.GREEN + "(DELETE)")
        print(Fore.YELLOW + "[5] " + Style.RESET_ALL + "Buscar Cliente por Nome " + Fore.BLUE + "(LIKE)")
        print(Fore.RED + "[0] " + Style.RESET_ALL + "Voltar ao Menu Principal")
        print(Fore.CYAN + "="*40 + Style.RESET_ALL)
        try:
            opcao = int(input(Fore.MAGENTA + "Escolha a opção: " + Style.RESET_ALL))
            if opcao == 1:
                ui_listar_clientes(con)
            elif opcao == 2:
                ui_cadastrar_cliente(con)
            elif opcao == 3:
                ui_editar_cliente(con)
            elif opcao == 4:
                ui_excluir_cliente(con)
            elif opcao == 5:
                ui_buscar_cliente_por_nome(con)
            elif opcao == 0:
                break
            else:
                print(Fore.RED + "Opção inválida." + Style.RESET_ALL)
                pause()
        except ValueError:
            print(Fore.RED + "Entrada inválida. Digite um número." + Style.RESET_ALL)
            pause()

def ui_buscar_cliente_por_nome(con):
    """Busca clientes usando o operador LIKE para nomes parciais."""
    clear_screen()
    try:
        termo_busca = input("Digite o nome ou parte do nome do cliente a buscar: ").strip()
        if not termo_busca:
            print(f"{Fore.YELLOW}Nenhum termo de busca fornecido.{Style.RESET_ALL}")
            return

        with con.cursor() as cur:
            # Demonstração do operador LIKE com '%' para busca parcial
            # A busca não diferencia maiúsculas/minúsculas (ILIKE)
            cur.execute(
                "SELECT id, nome, email, telefone FROM clientes WHERE nome ILIKE %s ORDER BY nome",
                (f'%{termo_busca}%',)
            )
            clientes = cur.fetchall()

        print(f"\n{Fore.CYAN}--- Resultados da Busca por '{termo_busca}' ---{Style.RESET_ALL}")
        if not clientes:
            print(f"{Fore.YELLOW}Nenhum cliente encontrado com o termo '{termo_busca}'.{Style.RESET_ALL}")
        else:
            print(Fore.CYAN + "{:<5} {:<25} {:<25} {:<15}".format("ID", "NOME", "EMAIL", "TELEFONE") + Style.RESET_ALL)
            print(Fore.YELLOW + "-"*75 + Style.RESET_ALL)
            for cliente in clientes:
                print("{:<5} {:<25} {:<25} {:<15}".format(
                    cliente[0], cliente[1][:24], cliente[2][:24], cliente[3] or ""
                ))
        log_evento("INFO", f"Busca por clientes com o termo '{termo_busca}' realizada.")
    except Exception as e:
        log_evento("ERROR", f"Erro ao buscar clientes: {e}")
        print(f"{Fore.RED}Erro ao realizar a busca: {e}{Style.RESET_ALL}")
    finally:
        pause()

def ui_editar_cliente(con):
    """UI 7: Editar Cliente (UPDATE)."""
    ui_listar_clientes(con)  # Mostra lista antes de pedir ID
    try:
        cliente_id = int(input("\nDigite o ID do cliente para editar: "))
        
        cur = con.cursor()
        cur.execute("SELECT * FROM clientes WHERE id = %s", (cliente_id,))
        cliente = cur.fetchone()
        
        if not cliente:
            print("Cliente não encontrado!")
            return
        # Solicita novos dados, mantendo os antigos se vazio    
        print(f"\nEditando cliente: {cliente[1]}")
        novo_nome = input(f"Novo nome ({cliente[1]}): ").strip() or cliente[1]
        novo_email = input(f"Novo email ({cliente[2]}): ").strip() or cliente[2]
        novo_tel = input(f"Novo telefone ({cliente[3] or 'Não cadastrado'}): ").strip() or cliente[3]
        # Atualiza os dados no banco
        cur.execute("""
            UPDATE clientes 
            SET nome = %s, email = %s, telefone = %s 
            WHERE id = %s""", (novo_nome, novo_email, novo_tel, cliente_id))
        con.commit()
        log_evento("INFO", f"Cliente ID {cliente_id} atualizado com sucesso.")
        print("\n✅ Cliente atualizado com sucesso!")
        
    except ValueError:
        log_evento("WARNING", "Entrada inválida (ID) na edição de cliente.")
        print("ID inválido!")
    except Exception as erro:
        print(f"Erro ao editar cliente: {erro}")
        con.rollback()

def ui_excluir_cliente(con):
    """UI: Excluir um cliente (DELETE)."""
    ui_listar_clientes(con, pausar=False) # Mostra a lista de clientes sem pausar
    try:
        cliente_id_str = input("\nDigite o ID do cliente que deseja excluir (ou deixe em branco para cancelar): ").strip()
        if not cliente_id_str:
            print("\nOperação de exclusão cancelada.")
            return

        if not cliente_id_str.isdigit():
            print(f"{Fore.RED}ID inválido. Por favor, digite um número.{Style.RESET_ALL}")
            return
        cliente_id = int(cliente_id_str)

        with con.cursor() as cur:
            cur.execute("SELECT id, nome, email FROM clientes WHERE id = %s", (cliente_id,))
            cliente = cur.fetchone()

            if not cliente:
                print(f"\n{Fore.YELLOW}Cliente com ID {cliente_id} não encontrado.{Style.RESET_ALL}")
                return

            print(f"\n{Fore.YELLOW}Você está prestes a excluir o seguinte cliente:{Style.RESET_ALL}")
            print(f"  ID:    {cliente[0]}")
            print(f"  Nome:  {cliente[1]}")
            print(f"  Email: {cliente[2]}")

            confirmacao = input(f"\n{Fore.RED}Tem certeza que deseja excluir este cliente? (S/N): {Style.RESET_ALL}").strip().upper()

            if confirmacao == 'S':
                cur.execute("DELETE FROM clientes WHERE id = %s", (cliente_id,))
                con.commit()
                log_evento("INFO", f"Cliente ID {cliente[0]} ('{cliente[1]}') foi excluído.")
                print(f"\n{Fore.GREEN}✅ Cliente excluído com sucesso.{Style.RESET_ALL}")
            else:
                log_evento("INFO", f"Exclusão do cliente ID {cliente[0]} foi cancelada pelo usuário.")
                print(f"\n{Fore.CYAN}Operação cancelada pelo usuário.{Style.RESET_ALL}")

    except Exception as erro:
        log_evento("ERROR", f"Erro ao tentar excluir cliente: {erro}")
        print(f"{Fore.RED}❌ Erro ao excluir cliente: {erro}{Style.RESET_ALL}")
        con.rollback()
    finally:
        pause()

def ui_cadastrar_cliente(con):
    """UI: Cadastrar novo cliente (CREATE)."""
    try:
        nome = input("Nome do cliente: ").strip()
        email = input("Email do cliente: ").strip()
        telefone = input("Telefone do cliente (opcional): ").strip()
        if not nome or not email:
            print("Nome e email são obrigatórios.")
            return
        cur = con.cursor()
        cur.execute(
            "INSERT INTO clientes (nome, email, telefone) VALUES (%s, %s, %s) RETURNING id",
            (nome, email, telefone if telefone else None)
        )
        new_id = cur.fetchone()[0]
        con.commit()
        log_evento("INFO", f"Cliente '{nome}' (ID: {new_id}) cadastrado com sucesso.")
        print(f"Cliente cadastrado com sucesso. ID: {new_id}")
    except Exception as erro:
        log_evento("ERROR", f"Erro ao cadastrar cliente: {erro}")
        print(f"Erro ao cadastrar cliente: {erro}")
        try:
            con.rollback()
        except Exception:
            pass

def ui_menu_pedidos(con):
    """UI 12: Menu de Gerenciamento de Pedidos."""
    while True:
        clear_screen()
        print("\n" + Fore.CYAN + "="*40)
        print("        MENU GERENCIAR PEDIDOS")
        print("="*40)
        print(Fore.YELLOW + "[1] " + Style.RESET_ALL + "Listar Pedidos " + Fore.GREEN + "(READ)")
        print(Fore.YELLOW + "[2] " + Style.RESET_ALL + "Cadastrar Novo " + Fore.GREEN + "(CREATE)")
        print(Fore.YELLOW + "[3] " + Style.RESET_ALL + "Editar Pedido " + Fore.GREEN + "(UPDATE)")
        print(Fore.YELLOW + "[4] " + Style.RESET_ALL + "Excluir Pedido " + Fore.GREEN + "(DELETE)")
        print(Fore.RED + "[0] " + Style.RESET_ALL + "Voltar ao Menu Principal")
        print(Fore.CYAN + "="*40 + Style.RESET_ALL)
        try:
            opcao = int(input(Fore.MAGENTA + "Escolha a opção: " + Style.RESET_ALL))
            if opcao == 1:
                ui_listar_pedidos(con)
            elif opcao == 2:
                ui_cadastrar_pedido(con)
            elif opcao == 3:
                ui_editar_pedido(con)
            elif opcao == 4:
                ui_excluir_pedido(con)
            elif opcao == 0:
                break
            else:
                print(Fore.RED + "Opção inválida." + Style.RESET_ALL)
        except ValueError:
            print(Fore.RED + "Entrada inválida. Digite um número." + Style.RESET_ALL)

def ui_editar_pedido(con):
    """UI: Edita o item e o valor de um pedido existente (UPDATE)."""
    ui_listar_pedidos(con)
    try:
        pedido_id_str = input("\nDigite o ID do pedido para editar (ou deixe em branco para cancelar): ").strip()
        if not pedido_id_str:
            return
        pedido_id = int(pedido_id_str)

        with con.cursor() as cur:
            cur.execute("SELECT id, item, valor FROM pedidos WHERE id = %s", (pedido_id,))
            pedido = cur.fetchone()

            if not pedido:
                print(f"{Fore.RED}Pedido com ID {pedido_id} não encontrado!{Style.RESET_ALL}")
                pause()
                return

            print(f"\nEditando Pedido ID: {pedido[0]}")
            novo_item = input(f"Novo item ({pedido[1]}): ").strip() or pedido[1]
            
            valor_atual_str = f"{pedido[2]:.2f}" if pedido[2] is not None else "Não cadastrado"
            novo_valor_str = input(f"Novo valor ({valor_atual_str}): ").strip()
            
            if novo_valor_str == "":
                novo_valor = pedido[2]
            else:
                novo_valor = float(novo_valor_str.replace(',', '.'))

            cur.execute(
                "UPDATE pedidos SET item = %s, valor = %s WHERE id = %s",
                (novo_item, novo_valor, pedido_id)
            )
        con.commit()
        log_evento("INFO", f"Pedido ID {pedido_id} atualizado. Novo item: '{novo_item}', Novo valor: {novo_valor:.2f}")
        print(f"\n{Fore.GREEN}✅ Pedido atualizado com sucesso!{Style.RESET_ALL}")

    except ValueError:
        log_evento("WARNING", "Entrada inválida (ID ou valor) na edição de pedido.")
        print(f"{Fore.RED}Entrada de ID ou valor inválida! Digite um número.{Style.RESET_ALL}")
        con.rollback()
    except Exception as erro:
        log_evento("ERROR", f"Erro ao editar pedido ID {pedido_id}: {erro}")
        print(f"{Fore.RED}Erro ao editar pedido: {erro}{Style.RESET_ALL}")
        con.rollback()
    finally:
        pause()

def ui_listar_pedidos(con):
    """UI: Lista todos os pedidos cadastrados com formatação brasileira."""
    clear_screen()
    try:
        cur = con.cursor()
        cur.execute("""
            SELECT p.id, c.id, c.nome, p.data_pedido, p.item, p.valor 
            FROM pedidos p 
            JOIN clientes c ON p.cliente_id = c.id
            ORDER BY p.data_pedido DESC
        """)
        pedidos = cur.fetchall()
        
        print("\n" + Fore.MAGENTA + "="*105 + Style.RESET_ALL)
        print(Fore.MAGENTA + "                        RELATÓRIO DE PEDIDOS" + Style.RESET_ALL)
        print(Fore.MAGENTA + "="*105 + Style.RESET_ALL)
        if pedidos:
            header = "{:<9} {:<10} {:<25} {:<25} {:<20} {:<12}".format(
                "ID Pedido", "ID Cliente", "CLIENTE", "DATA", "ITEM", "VALOR (R$)"
            )
            print(Fore.CYAN + header + Style.RESET_ALL)
            print(Fore.YELLOW + "-"*105 + Style.RESET_ALL)
            for pedido in pedidos:
                try:
                    # data_pedido is now at index 3
                    data_formatada = pedido[3].strftime("%d de %B de %Y")
                except:
                    data_formatada = pedido[3].strftime("%d/%m/%Y")
                
                # Tratamento para valores nulos (None)
                item_str = (pedido[4] or "N/D")[:19]
                valor_num = pedido[5] if pedido[5] is not None else 0.0
                valor_str = f"R$ {valor_num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

                print(Fore.WHITE + "{:<9} {:<10} {:<25} {:<25} {:<20} ".format(
                    pedido[0], pedido[1], pedido[2][:24], data_formatada, item_str
                ) + Fore.GREEN + f"{valor_str:<12}" + Style.RESET_ALL)
            print(Fore.MAGENTA + "="*105 + Style.RESET_ALL)
        else:
            print(Fore.YELLOW + "\nNenhum pedido cadastrado." + Style.RESET_ALL)
            print(Fore.MAGENTA + "="*105 + Style.RESET_ALL)
            log_evento("DEBUG", "Listagem de pedidos executada: Nenhum pedido encontrado.")
    except Exception as erro:
        print(Fore.RED + f"Erro ao listar pedidos: {erro}" + Style.RESET_ALL)
    finally:
        pause()

def ui_cadastrar_pedido(con):
    """UI: Cadastrar novo pedido (CREATE)."""
    # Mostra a lista de clientes para ajudar na escolha do ID
    ui_listar_clientes(con, pausar=False)
    try:
        # Adiciona uma linha em branco para separar da lista
        cliente_id_str = input("\nDigite o ID do cliente para o novo pedido: ").strip()
        if not cliente_id_str.isdigit():
            print("ID de cliente inválido.")
            return
        cliente_id = int(cliente_id_str)

        valor_str = input("Valor do pedido (ex: 123.45) [opcional]: ").strip()
        if valor_str == "":
            valor = None
        else:
            try:
                valor = float(valor_str)
            except ValueError:
                print("Valor inválido.")
                return
        item = input("Descrição do item do pedido: ").strip()
        if not item:    
            print("Descrição do item é obrigatória.")
            return

        cur = con.cursor()
        cur.execute("SELECT id FROM clientes WHERE id = %s", (cliente_id,))
        if cur.fetchone() is None:
            print("Cliente não encontrado.")
            return

        cur.execute(
            "INSERT INTO pedidos (cliente_id, valor, item) VALUES (%s, %s, %s) RETURNING id",
            (cliente_id, valor, item)
        )
        new_id = cur.fetchone()[0]
        con.commit()
        log_evento("INFO", f"Pedido ID {new_id} para o cliente ID {cliente_id} cadastrado com sucesso.")
        print(f"Pedido cadastrado com sucesso. ID: {new_id}")
        pause()
    except Exception as erro:
        log_evento("ERROR", f"Erro ao cadastrar pedido: {erro}")
        print(f"Erro ao cadastrar pedido: {erro}")
        pause()
        try:
            con.rollback()
        except Exception:
            pass

def ui_listar_clientes(con, pausar=True):
    """UI 5: Lista todos os clientes cadastrados."""
    clear_screen()
    try:
        cur = con.cursor()
        cur.execute("SELECT id, nome, email, telefone FROM clientes ORDER BY id")
        clientes = cur.fetchall()
        
        print("\n" + Fore.MAGENTA + "="*75 + Style.RESET_ALL)
        print(Fore.MAGENTA + "                        LISTA DE CLIENTES" + Style.RESET_ALL)
        print(Fore.MAGENTA + "="*75 + Style.RESET_ALL)
        if clientes:
            print(Fore.CYAN + "{:<5} {:<25} {:<25} {:<15}".format("ID", "NOME", "EMAIL", "TELEFONE") + Style.RESET_ALL)
            print(Fore.YELLOW + "-"*75 + Style.RESET_ALL)
            for cliente in clientes:
                print("{:<5} {:<25} {:<25} {:<15}".format(
                    cliente[0], 
                    cliente[1][:24], 
                    cliente[2][:24], 
                    cliente[3] or ""))
        else:
            print(Fore.YELLOW + "Nenhum cliente cadastrado." + Style.RESET_ALL)
        print(Fore.MAGENTA + "="*75 + Style.RESET_ALL)
        log_evento("DEBUG", "Listagem de clientes executada.")
    except Exception as erro:
        print(Fore.RED + f"Erro ao listar clientes: {erro}" + Style.RESET_ALL)
    finally:
        if pausar:
            pause()

def ui_excluir_pedido(con):
    """UI: Excluir um pedido (DELETE)."""
    try:
        pedido_id_str = input("ID do pedido a excluir: ").strip()
        if not pedido_id_str.isdigit():
            print("ID inválido.")
            return
        pedido_id = int(pedido_id_str)

        cur = con.cursor()
        cur.execute("DELETE FROM pedidos WHERE id = %s", (pedido_id,))
        if cur.rowcount > 0:
            con.commit()
            log_evento("INFO", f"Pedido ID {pedido_id} foi excluído.")
            print("Pedido excluído com sucesso.")
        else:
            print("Pedido não encontrado.")
    except Exception as erro:
        log_evento("ERROR", f"Erro ao excluir pedido: {erro}")
        print(f"Erro ao excluir pedido: {erro}")
        try:
            con.rollback()
        except Exception:
            pass

def ui_menu_especial(con):
    """UI 15: Menu de Funcionalidades Especiais."""
    while True:
        #clear_screen()
        print("\n" + Fore.CYAN + "="*45)
        print("        MENU DE IMPORTAÇÃO E EXPORTAÇÃO")
        print("="*45 + Style.RESET_ALL)
        print(Fore.MAGENTA + "[1] " + Fore.CYAN + "Exportar Dados (JSON + ZIP)📦")
        print(Fore.MAGENTA + "[2] " + Fore.CYAN + "Importar Dados da Web 🌐")
        print(Fore.MAGENTA + "[3] " + Fore.CYAN + "Importar Dados de Arquivo Local 📁")
        print(Fore.MAGENTA + "[4] " + Fore.CYAN + "Processar Dados Importados ⚙️")
        print(Fore.MAGENTA + "[5] " + Fore.CYAN + "Visualizar Logs 📜")
        print(Fore.RED + "[0] " + Fore.CYAN + "Voltar ao Menu Principal" + Style.RESET_ALL)
        print(Fore.CYAN + "="*45 + Style.RESET_ALL)
        try:
            opcao = int(input("Escolha a opção: "))
            if opcao == 1:
                ui_exportar_dados(con)
            elif opcao == 2:
                ui_importar_dados(con)
            elif opcao == 3:
                ui_importar_dados_local(con)
            elif opcao == 4:
                ui_processar_dados_importados(con)
            elif opcao == 5:
                ui_visualizar_logs(con)
            elif opcao == 0:
                break
            else:
                print("Opção inválida.")
        except ValueError:
            print("Entrada inválida. Digite um número.")

def ui_importar_dados_local(con):
    """Lista arquivos .json no diretório local e importa o escolhido para o banco."""
    clear_screen()
    try:
        print(f"{Fore.CYAN}Buscando arquivos .json no diretório atual:{Style.RESET_ALL} {os.getcwd()}")
        
        # Lista arquivos .json no diretório
        json_files = [f for f in os.listdir('.') if f.endswith('.json')]

        if not json_files:
            print(f"\n{Fore.YELLOW}Nenhum arquivo .json encontrado no diretório atual.{Style.RESET_ALL}")
            pause()
            return

        print(f"\n{Fore.GREEN}Arquivos JSON encontrados:{Style.RESET_ALL}")
        for i, filename in enumerate(json_files, 1):
            print(f"  [{i}] {filename}")

        escolha_str = input("\nDigite o número do arquivo que deseja importar (ou 0 para cancelar): ").strip()
        if not escolha_str.isdigit() or int(escolha_str) == 0:
            print("Importação cancelada.")
            return

        escolha_idx = int(escolha_str) - 1

        if 0 <= escolha_idx < len(json_files):
            arquivo_escolhido = json_files[escolha_idx]
            print(f"Importando '{arquivo_escolhido}'...")

            with open(arquivo_escolhido, 'r', encoding='utf-8') as f:
                dados_json = json.load(f)

            with con.cursor() as cur:
                cur.execute(
                    "INSERT INTO dados_importados (dado_json) VALUES (%s) RETURNING id",
                    (json.dumps(dados_json),)
                )
                new_id = cur.fetchone()[0]
            con.commit()

            log_evento("INFO", f"Dados do arquivo local '{arquivo_escolhido}' importados com ID {new_id}.")
            print(f"{Fore.GREEN}✅ Arquivo '{arquivo_escolhido}' importado com sucesso! ID do registro: {new_id}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Escolha inválida.{Style.RESET_ALL}")

    except Exception as e:
        log_evento("ERROR", f"Falha ao importar arquivo local: {e}")
        print(f"{Fore.RED}❌ Erro ao importar arquivo local: {e}{Style.RESET_ALL}")
    finally:
        pause()

# MENU DE PROCESSAMENTO DE DADOS IMPORTADOS
# =========================================================================
# FUNÇÕES DE INTERFACE DO USUÁRIO (UI 3) - Adicionadas
# =========================================================================
def ui_processar_dados_importados(con):
    """Interface para visualizar, validar e importar dados da tabela 'dados_importados'."""
    while True:
        clear_screen()
        try:
            with con.cursor() as cur:
                # Demonstração de fetchall
                cur.execute("SELECT id, status, dado_json->>'name' as nome FROM dados_importados ORDER BY id")
                registros = cur.fetchall() # Lista de tuplas (id, status, nome)

            print("\n" + Fore.MAGENTA + "="*60 + Style.RESET_ALL)
            print(Fore.MAGENTA + "        PROCESSAMENTO DE DADOS IMPORTADOS" + Style.RESET_ALL)
            print(Fore.MAGENTA + "="*60 + Style.RESET_ALL)

            if not registros:
                print(Fore.YELLOW + "Nenhum dado importado para processar." + Style.RESET_ALL)
                pause()
                return

            print(Fore.CYAN + "{:<5} {:<15} {}".format("ID", "STATUS", "NOME (do JSON)") + Style.RESET_ALL)
            print(Fore.YELLOW + "-"*60 + Style.RESET_ALL)
            for reg in registros:
                status_cor = Fore.GREEN if reg[1] == 'NOVO' else Fore.WHITE
                print(status_cor + "{:<5} {:<15} {}".format(reg[0], reg[1], reg[2] or "N/A") + Style.RESET_ALL)

            print("\n" + Fore.CYAN + "Digite o ID do registro para processar ou '0' para voltar." + Style.RESET_ALL)
            id_str = input("Escolha o ID: ").strip()

            if id_str == '0':
                break
            
            id_registro = int(id_str)
            
            # Demonstração de fetchone
            with con.cursor() as cur:
                cur.execute("SELECT dado_json, status FROM dados_importados WHERE id = %s", (id_registro,))
                registro_selecionado = cur.fetchone()

            if not registro_selecionado:
                print(f"{Fore.RED}ID {id_registro} não encontrado.{Style.RESET_ALL}")
                pause()
                continue
            
            dado_json, status = registro_selecionado
            if status != 'NOVO':
                print(f"{Fore.YELLOW}O registro ID {id_registro} já foi processado.{Style.RESET_ALL}")
                pause()
                continue

            print("\n" + Fore.BLUE + "--- Conteúdo do JSON Selecionado ---" + Style.RESET_ALL)
            print(json.dumps(dado_json, indent=2, ensure_ascii=False))
            print(Fore.BLUE + "------------------------------------" + Style.RESET_ALL)

            confirmacao = input(f"\n{Fore.YELLOW}Deseja processar e sincronizar clientes e pedidos deste arquivo? (S/N): {Style.RESET_ALL}").strip().upper()

            if confirmacao == 'S':
                clientes_inseridos = 0
                clientes_ignorados = 0
                pedidos_inseridos = 0
                pedidos_ignorados = 0

                with con.cursor() as cur:
                    # Processar Clientes
                    if 'clientes' in dado_json and isinstance(dado_json['clientes'], list):
                        for cliente_data in dado_json['clientes']:
                            cur.execute("SELECT id FROM clientes WHERE id = %s", (cliente_data.get('id'),))
                            if cur.fetchone():
                                clientes_ignorados += 1
                            else:
                                cur.execute(
                                    "INSERT INTO clientes (id, nome, email, telefone) VALUES (%s, %s, %s, %s)",
                                    (cliente_data.get('id'), cliente_data.get('nome'), cliente_data.get('email'), cliente_data.get('telefone'))
                                )
                                clientes_inseridos += 1

                    # Processar Pedidos
                    if 'pedidos' in dado_json and isinstance(dado_json['pedidos'], list):
                        for pedido_data in dado_json['pedidos']:
                            cur.execute("SELECT id FROM pedidos WHERE id = %s", (pedido_data.get('id'),))
                            if cur.fetchone():
                                pedidos_ignorados += 1
                            else:
                                # Verifica se o cliente do pedido existe antes de inserir
                                cur.execute("SELECT id FROM clientes WHERE id = %s", (pedido_data.get('cliente_id'),))
                                if cur.fetchone():
                                    cur.execute(
                                        "INSERT INTO pedidos (id, cliente_id, data_pedido, item, valor) VALUES (%s, %s, %s, %s, %s)",
                                        (pedido_data.get('id'), pedido_data.get('cliente_id'), pedido_data.get('data_pedido'), pedido_data.get('item'), pedido_data.get('valor'))
                                    )
                                    pedidos_inseridos += 1
                                else:
                                    pedidos_ignorados += 1 # Ignora pedido se o cliente não existe

                    # Atualiza o status do registro JSON para 'PROCESSADO'
                    cur.execute("UPDATE dados_importados SET status = 'PROCESSADO' WHERE id = %s", (id_registro,))

                    # Atualiza as sequências para evitar conflitos de ID no futuro
                    if clientes_inseridos > 0:
                        cur.execute("SELECT setval('clientes_id_seq', (SELECT MAX(id) FROM clientes));")
                    if pedidos_inseridos > 0:
                        cur.execute(
                            "SELECT setval('pedidos_id_seq', (SELECT MAX(id) FROM pedidos));"
                        )
                con.commit()

                print(f"\n{Fore.GREEN}--- Relatório de Sincronização ---{Style.RESET_ALL}")
                print(f"Clientes Inseridos: {Fore.GREEN}{clientes_inseridos}{Style.RESET_ALL}")
                print(f"Clientes Ignorados (já existiam): {Fore.YELLOW}{clientes_ignorados}{Style.RESET_ALL}")
                print(f"Pedidos Inseridos: {Fore.GREEN}{pedidos_inseridos}{Style.RESET_ALL}")
                print(f"Pedidos Ignorados (já existiam ou cliente não encontrado): {Fore.YELLOW}{pedidos_ignorados}{Style.RESET_ALL}")
                
                log_msg = f"JSON ID {id_registro} processado. Clientes: {clientes_inseridos} inseridos, {clientes_ignorados} ignorados. Pedidos: {pedidos_inseridos} inseridos, {pedidos_ignorados} ignorados."
                log_evento("INFO", log_msg)

            else:
                print(f"{Fore.CYAN}Importação cancelada pelo usuário.{Style.RESET_ALL}")

            pause()

        except ValueError:
            print(f"{Fore.RED}Entrada inválida. Por favor, digite um número de ID.{Style.RESET_ALL}")
            pause()
        except Exception as e:
            print(f"{Fore.RED}Erro ao processar dados: {e}{Style.RESET_ALL}")
            con.rollback()
            pause()

def ui_visualizar_logs(con):
    """Exibe os últimos logs registrados no banco de dados."""
    clear_screen()
    try:
        with con.cursor() as cur:
            # Busca os 50 logs mais recentes
            cur.execute("SELECT id, timestamp, level, message FROM logs ORDER BY timestamp DESC LIMIT 50")
            logs = cur.fetchall()

        print("\n" + Fore.MAGENTA + "="*100 + Style.RESET_ALL)
        print(Fore.MAGENTA + "                        VISUALIZADOR DE LOGS (ÚLTIMOS 50 EVENTOS)" + Style.RESET_ALL)
        print(Fore.MAGENTA + "="*100 + Style.RESET_ALL)

        if not logs:
            print(Fore.YELLOW + "Nenhum log encontrado." + Style.RESET_ALL)
        else:
            header = "{:<5} {:<25} {:<10} {}".format("ID", "DATA/HORA", "NÍVEL", "MENSAGEM")
            print(Fore.CYAN + header + Style.RESET_ALL)
            print(Fore.YELLOW + "-"*100 + Style.RESET_ALL)
            for log in logs:
                # Formata o timestamp para exibição
                timestamp_fmt = log[1].strftime('%Y-%m-%d %H:%M:%S')
                print("{:<5} {:<25} {:<10} {}".format(log[0], timestamp_fmt, log[2], log[3]))
        print(Fore.MAGENTA + "="*100 + Style.RESET_ALL)
    except Exception as e:
        print(f"{Fore.RED}Erro ao buscar logs: {e}{Style.RESET_ALL}")
    finally:
        pause()

def ui_exportar_dados(con):
    """Exporta dados das tabelas clientes e pedidos para um arquivo JSON e compacta em ZIP."""
    try:
        cur = con.cursor()
        cur.execute("SELECT id, nome, email, telefone FROM clientes ORDER BY id")
        clientes = cur.fetchall()
        cur.execute("SELECT id, cliente_id, data_pedido, item, valor FROM pedidos ORDER BY id")
        pedidos = cur.fetchall()

        dados = {
            "clientes": [
                {"id": c[0], "nome": c[1], "email": c[2], "telefone": c[3]} for c in clientes
            ],
            "pedidos": [
                {"id": p[0], "cliente_id": p[1], "data_pedido": str(p[2]), "item": p[3], "valor": float(p[4]) if p[4] is not None else None} for p in pedidos
            ]
        }
        # Gera nomes de arquivos com timestamp para evitar sobrescrita
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # Formato para o nome do arquivo
        json_filename = f"exportacao_{timestamp}.json"
        zip_filename = f"exportacao_{timestamp}.zip"

        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)

        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(json_filename)

        log_evento("INFO", f"Dados exportados com sucesso para o arquivo '{zip_filename}'.")
        print(Fore.GREEN + f"Dados exportados para {json_filename} e compactados em {zip_filename}." + Style.RESET_ALL)
    except Exception as erro:
        log_evento("ERROR", f"Falha ao exportar dados: {erro}")
        print(Fore.RED + f"Erro ao exportar dados: {erro}" + Style.RESET_ALL)

# UI 18: Importar Dados da Web (Requisito obrigatório).
def ui_importar_dados(con):
    """Importa dados de uma fonte web e armazena na tabela dados_importados."""
    try:
        # URL de teste pública que retorna JSON
        url_exemplo = 'https://generatedata.com/data/...'

        print(f"Acesse {Fore.CYAN}generatedata.com{Style.RESET_ALL}, gere seus dados e cole a URL completa aqui.")
        url = input(f"URL (ex: {url_exemplo}): ").strip()

        # Se o usuário não digitar nada, usa a URL padrão
        if not url:
            print(f"{Fore.YELLOW}Nenhuma URL fornecida. Operação cancelada.{Style.RESET_ALL}")
            log_evento("INFO", "Importação da web cancelada por falta de URL.")
            return

        # Garante que o esquema esteja presente
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # ===== Requisição Web =====
        print(Fore.BLUE + f"🔗 Acessando URL: {url}" + Style.RESET_ALL)
        response = requests.get(url)
        print(Fore.BLUE + f"📄 Código de Status HTTP: {response.status_code}" + Style.RESET_ALL)

        # Tratamento de erros detalhado lista de status codes comuns e suas mensagens
        if response.status_code == 200:
            print("✅ Sucesso! Requisição concluída.")
        elif response.status_code == 201:
            print("✅ Recurso criado com sucesso.")
        elif response.status_code == 204:
            print("ℹ️ Requisição bem-sucedida, mas sem conteúdo.")
        elif response.status_code == 301:
            raise ValueError("🔁 Erro 301: O recurso foi movido permanentemente.")
        elif response.status_code == 302:
            raise ValueError("🔁 Erro 302: O recurso foi movido temporariamente.")
        elif response.status_code == 400:
            raise ValueError("❌ Erro 400: Requisição inválida.")
        elif response.status_code == 401:
            raise ValueError("🔒 Erro 401: Acesso não autorizado.")
        elif response.status_code == 403:
            raise ValueError("🚫 Erro 403: Acesso proibido.")
        elif response.status_code == 404:
            raise ValueError("🔍 Erro 404: URL não encontrada.")
        elif response.status_code == 408:
            raise ValueError("⏰ Erro 408: Tempo de requisição esgotado.")
        elif response.status_code == 429:
            raise ValueError("⚠️ Erro 429: Muitas requisições — tente novamente mais tarde.")
        elif response.status_code == 500:
            raise ValueError("💥 Erro 500: Erro interno no servidor.")
        elif response.status_code == 502:
            raise ValueError("💥 Erro 502: Gateway inválido.")
        elif response.status_code == 503:
            raise ValueError("💥 Erro 503: Serviço indisponível.")
        elif response.status_code == 504:
            raise ValueError("💥 Erro 504: Tempo de resposta esgotado.")
        else:
            raise ValueError(f" Erro desconhecido. Código de status: {response.status_code}")

        # Verifica se o conteúdo retornado é JSON
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            raise ValueError(f" O conteúdo retornado não é JSON. Tipo encontrado: {content_type}")

        print(Fore.GREEN + "✅ Dados obtidos com sucesso da web." + Style.RESET_ALL)

        # ===== Inserção no Banco =====
        dado_json = response.json()
        cur = con.cursor()
        cur.execute(
            "INSERT INTO dados_importados (dado_json) VALUES (%s) RETURNING id",
            (json.dumps(dado_json),)
        )
        new_id = cur.fetchone()[0]
        con.commit()

        log_evento("INFO", f"Dados importados da web ({url}) e salvos no banco com ID {new_id}.")
        print(Fore.GREEN + f"✅ Dados importados e salvos com sucesso. ID: {new_id}" + Style.RESET_ALL)

    except requests.exceptions.MissingSchema:
        msg_erro = "Esquema de URL ausente (use http:// ou https://)."
        log_evento("ERROR", f"Erro de importação: {msg_erro}")
        print(Fore.RED + f"Erro: {msg_erro}" + Style.RESET_ALL)
        try:
            con.rollback()
        except Exception:
            pass
    except requests.exceptions.RequestException as e:
        log_evento("ERROR", f"Erro de requisição na importação: {e}")
        print(Fore.RED + f"❌ Erro de requisição: {e}" + Style.RESET_ALL)
        try:
            con.rollback()
        except Exception:
            pass
    except ValueError as e:
        log_evento("ERROR", f"Erro de valor na importação: {e}")
        print(Fore.RED + f"⚠️ {e}" + Style.RESET_ALL)
        try:
            con.rollback()
        except Exception:
            pass
    except Exception as erro:
        log_evento("CRITICAL", f"Erro inesperado ao importar dados: {erro}")
        print(Fore.RED + f"💥 Erro ao importar dados: {erro}" + Style.RESET_ALL)
        try:
            con.rollback()
        except Exception:
            pass


def clear_screen():
    """Limpa a tela do terminal (compatível com Windows e Unix)."""
    os.system('cls' if os.name == 'nt' else 'clear')

def pause():
    """Aguarda o usuário pressionar ENTER para continuar."""
    input("\nPressione ENTER para continuar...")

def main():
    """Função principal que gerencia o ciclo de vida da aplicação."""
    try:
        # Inicializa o pool e cria as tabelas se necessário
        log_evento("INFO", "Aplicação iniciada.")
        DatabasePool.get_pool()
        criar_tabelas()
        
        # As funções de UI agora usam a conexão antiga, vamos passar para elas
        conexao = conectar_db()
        if conexao and ui_login(conexao):
            ui_menu_principal(conexao)
    except Exception as erro:
        log_evento("CRITICAL", f"Erro fatal na aplicação: {erro}")
        print(f"Erro fatal: {erro}")
    finally:
        DatabasePool.close_all()

# Execução do programa (teste inicial)
if __name__ == "__main__":
    main()