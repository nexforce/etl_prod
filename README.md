# DOCUMENTAÇÃO DO PROCESSO DE ETL

## VISÃO GERAL DO PROCESSO

### Contexto
O processo de ETL consiste em buscar dados das plataformas ClickUp e Hubspot e traduzi-los em informações, visualizações e insights no Looker. Para fazer isso, de maneira automatizada, utiliza-se uma Máquina Virtual (VM) dentro do Google Cloud. Em resumo, dentro da VM é feita a extração dos dados dentro das plataformas, transformação destes dados e o carregamento final nas tabelas dimensão e fato, que alimentam o Looker.

### 1. Máquina Virtual
Há uma máquina virtual (VM) rodando no servidor da Google, onde todo o processo de ETL acontece. A Máquina possui recursos como memória e poder de processamento para realizar as tarefas necessárias. Essa VM é onde o processo será orquestrado, sem precisar de interferência manual constante. A única parte manual consiste na abertura da instância da VM antes do horário marcado para executar o ETL. Este processo está detalhadamente explicado mais abaixo.

### 2. Coletando os Dados - Extração
O primeiro passo é obter dados das tarefas do ClickUp e da Hubspot, que são armazenados na plataforma de gerenciamento. A cada fim de sprint, um script rodando na máquina virtual vai até a API do ClickUp e da Hubspot buscar todas as tarefas, atualizações dos customers e dos squads. Esses dados são coletados de maneira simples e bruta e colocados em um "estoque temporário" dentro da máquina, um espaço de armazenamento onde essas informações ficam esperando o próximo passo.

### 3. Preparando os Dados - Transformação  
Agora, com as informações em mãos, é hora de dar forma a esses dados. A transformação é a parte onde você ajusta as informações para que fiquem prontas para análise.  
Durante esse passo:  
- Variáveis como datas são ajustadas para o formato correto.
- Propriedades principais são escolhidas.  
- Ajustes nos tipos de dados são realizados.  
- Os dados são organizados, como traduzir status ou categorizar as tarefas.  
- Em alguns casos, como para obter as informações de Service que cada cliente utiliza, até cálculos podem ser feitos, como somar valores e receitas para cada serviço e também os pontos contratados.

### 4. Carregando os Dados - Carga 
Após a transformação, os dados organizados são levados até a base de dados principal (o BigQuery). Esse é o banco onde todos os dados são armazenados de maneira permanente. A carga de dados no BigQuery acontece de forma ordenada, sem misturar dados novos e dados antigos, para garantir que tudo fique organizado e fácil de acessar. Esse carregamento no BigQuery cria as chamadas tabelas de *Stage*, que são tabelas provisórias em que os dados são organizados, antes de serem alimentados para as tabelas dimensão e fato.

Em seguida, são executadas queries em que os dados da Stage vão para as tabelas `dim` e `fat`. Nesse processo, as queries são organizadas de forma que as tabelas finais adotem o *slowly changing dimension (SCD)*, que consiste em técnicas para gerenciamento da história de dados dimensionais. Ou seja, todas as etapas que houver mudanças nas propriedades de squads e clientes, elas serão registradas.

### 5. Tasks no Sprint History
Como não podem ser movidas via script, as tasks obtidas nos passos anteriores serão recriadas na pasta *Sprint History* do ClickUp. Este passo é importante para garantir que tenhamos um histórico de tarefas salvas. E é realizado na última etapa do script, antes da instância ser desligada.

### 6. Looker Data Studio
O Looker é alimentado ao final do passo 4. Ou seja, o passo 5 não precisa estar terminado para que o Looker já possa ser acessado com os novos dados do fim de sprint. Portanto, quando as tabelas dimensão e fato estiverem completamente concluídas, o Looker será atualizado de maneira instantânea, de forma já disponível para ser acessado pelos membros da Nexforce.

---

## COMO CRIAR A VM

1. **Acesse o Google Cloud**  
   No menu do Google Cloud, acesse a opção **“Compute Engine”** e clique em **“Instâncias de VM”**.

2. **Crie a Instância**  
   Clique em **“Criar Instância”** e configure os seguintes parâmetros:
   - **Nome da Instância**: Escolha um nome único.
   - **Região e Zona**: Escolha onde a VM será criada (ex.: `us-central1`).
   - **Tipo de Máquina**: Configure os recursos:
     - Recomendada: `e2-standard-2`.
   - **Imagem do Sistema Operacional**: Escolha (ex.: Ubuntu, Debian, CentOS ou Windows).
     - Clique em “Alterar” para selecionar a imagem desejada.
   - **Disco de Inicialização**:
     - Defina o tamanho do disco (ex.: 10 GB para testes).
     - Escolha o tipo de disco (SSD é mais rápido, mas HDD é mais barato).
   - **Rede**:
     - Escolha a rede padrão ou configure uma rede personalizada.
     - Ative a opção de IP externo, se necessário.

3. **Configurações de Rede e Firewall**  
   Ative as opções de firewall para conexões HTTP/HTTPS, se necessário.

4. **Criar Instância**  
   Clique em **“Criar”** e, após alguns segundos, sua VM estará pronta.

5. **Acessar via SSH**  
   No painel de Instâncias de VM, localize a máquina criada e clique em **“SSH”** para continuar.

---

## COMO CONFIGURAR O ETL NA VM

### 1. Atualização do sistema e instalação de pacotes básicos
```bash
sudo apt update
sudo apt install python3-pip
sudo apt install python3-venv

### 2. Configuração do ambiente virtual

```bash
python3 -m venv myenv
source myenv/bin/activate

markdown
Copiar código
## Configuração do Ambiente

### 2. Configuração do ambiente virtual

```bash
python3 -m venv myenv
source myenv/bin/activate
python3 -m venv myenv: Cria o ambiente virtual.
source myenv/bin/activate: Ativa o ambiente virtual, garantindo que todas as dependências sejam instaladas isoladamente nesse ambiente.

### 3. Instalação do Git e clonagem do repositório
bash
Copiar código
sudo apt install git
git clone https://github.com/nexforce/etl_prod.git
cd etl_prod

sudo apt install git: Instala o Git, uma ferramenta essencial para gerenciar repositórios de código.

git clone: Clona o repositório do projeto, baixando os arquivos necessários.

cd etl_prod: Acessa o diretório onde os arquivos do projeto foram baixados.

### 4. Instalação das dependências do projeto

`pip install -r requirements.txt `

O arquivo requirements.txt no repositório clonado contém a lista de bibliotecas que o projeto necessita. Este comando garante que o ambiente está preparado.

### 5. Configuração do Google Cloud

`gcloud config set project YOUR_PROJECT_ID`

Define o contexto do Google Cloud para que as operações (como transferência de arquivos e execução de jobs) sejam realizadas no projeto correto.

### 6. Transferência de arquivos credentials e .env para a VM

```
gcloud compute scp "C:\caminho\caminho\bigquery_credentials.json" nome_do_usuario@nome_da_vm:/home/nome_do_usuario/

gcloud compute scp "C:\caminho\caminho\.env" nome_do_usuario@nome_da_vm:/home/nome_do_usuario/nome_do_diretorio/

```

bigquery_credentials.json: Arquivo de credenciais para autenticação no BigQuery.

.env: Arquivo contendo variáveis de ambiente do projeto, como chaves API, senhas, etc.

O comando scp copia os arquivos do computador local para a VM na pasta especificada.

O arquivo de credenciais vai para /home/ricardo_semerene/, enquanto .env é movido diretamente para /home/ricardo_semerene/etl_prod/.

### 7. Movimentação e organização de arquivos na VM
```
cd..
mv bigquery_credentials.json etl_prod/
cd etl_prod
ls
```

cd..: Sobe um nível na hierarquia de diretórios.

mv bigquery_credentials.json etl_prod/: Move o arquivo de credenciais para o diretório do projeto.

cd etl_prod: Retorna ao diretório do projeto.

ls: Lista os arquivos no diretório para confirmar que tudo está no lugar.

## COMO LIGAR A INSTÂNCIA

Para ligar a instância, basta acessar no menu do Google Cloud a opção “Compute Engine”. Pode clicar diretamente ou clicar no primeiro ícone “Instâncias de VM”. Em seguida, localize e selecione a instância de nome “etl” e clique em “Iniciar/Retomar”. Aguarde alguns segundos e a instância está ligada !

## COMO ADICIONAR/EDITAR UM ALERTA

Para criar um alerta, basta acessar o menu do Google Cloud e passar o mouse sobre a opção “Monitoramento”. Em seguida, acesse “Alertas” dentro da aba “Detectar”. No canto superior, clique em “create policy” e agora é só configurar o alerta. 

Primeiramente, selecione a métrica que vai o alerta vai se basear, na aba “New Condition”. Configure o gatilho, apontando qual o valor do seu treshold. O treshold é qual o valor limite da sua métrica você deseja receber o alerta. Isso é feito logo abaixo em “Configurar Gatilho”. Na aba “Notificações e Nome” você vai sinalizar para qual canal de comunicação deseja que o alerta seja enviado. 


## COMO PROGRAMAR/EDITAR UM HORÁRIO COM O CRON

Para editar o horário de execução do ETL com o cron, basta abrir a instância e abrir a janela do navegador. Para fazer isso, é só localizar no canto direito em “conectar” e clicar em cima de “SSH”. Dentro do prompt da VM, digite o comando 

```
crontab -e
```

No arquivo, na última linha, estará escrito:

```
32 22 * * * /home/ricardo_semerene/etl_prod/venv/bin/python /home/ricardo_semerene/etl_prod/main.py >> /home/ricardo_semerene/etl_prod/etl_log.txt 2>&1`
```

Se estiver em branco, adicione esta linha ao final das linhas comentadas.
A primeira dezena refere-se aos minutos do horário que o código será executado. Já a segunda dezena refere-se às horas. 

Cuidado que há um fuso horário, dependendo de onde você estiver. O horário do cron está alinhado ao da VM. Para saber o horário da VM basta sair do arquivo cron (Ctrl + X) e digitar na parte inicial do prompt da VM “date”

Sabendo disso, é só voltar ao arquivo cron e fazer a modificação nessas duas primeiras dezenas, para editar o horário. Salve com Ctrl + O + Enter e depois sai com Ctril + X.  


## COMO EDITAR UM ARQUIVO DENTRO DA VM

Para editar um arquivo dentro da VM, basta abrir a instância e abrir a janela do navegador. Para fazer isso, é só localizar no canto direito em “conectar” e clicar em cima de “SSH”. Dentro do prompt da VM, digite o comando 

```
nano /home/ricardo_semerene/etl_prod/nome_do_arquivo`
```
exemplo para o arquivo main.py:
`
```
nano /home/ricardo_semerene/etl_prod/main.py`
```
Dessa forma conseguirá acessar o arquivo e poderá alterá-lo. Para salvar aperte “Ctrl + O” e depois “Enter”. E para sair digite “Ctrl X”.


## COMO SALVAR E ATUALIZAR O GITHUB

Para salvar e atualizar no Github:

vá até o SSH da instância e entre no diretório do código com o comando

```
cd etl_prod`
```
com “git status” você pode conferir como estão os arquivos. Se falta salvar, fazer um commit etc.

Após fazer uma mudança ou algumas mudanças no código, digite `“git add . “`

Em seguida, é hora de fazer o commit com 

```
git commit -m “nome do commit”`
```
e por fim enviar o commit pro repositório remoto localizado em: https://github.com/nexforce/etl_prod, 
digite:
```
git push origin main`
```
## COMO ACESSAR O ARQUIVO DE LOGS

Para acessar o arquivo de logs dentro da VM, basta abrir a instância e abrir a janela do navegador. Para fazer isso, é só localizar no canto direito em “conectar” e clicar em cima de “SSH”. Dentro do prompt da VM, digite o comando

```
tail -f /home/ricardo_semerene/etl_prod/log.txt`
```
Este comando vai exibir as últimas linhas do arquivo em tempo real. Também é possível especificar o número de linhas:

```
tail -n 20 log.txt`
```
Este comando exibe as 20 últimas linhas. 

Cuidado ao tentar abrir este arquivo completo, pois ele é muito grande e pode “travar” o terminal. Mas, se for o caso, o comando é o

```
cat log.txt`
```
## COMO EXECUTAR E INTERROMPER O SCRIPT MANUALMENTE

Para executar o código manualmente, basta abrir a instância e abrir a janela do navegador. Para fazer isso, é só localizar no canto direito em “conectar” e clicar em cima de “SSH”. Dentro do prompt da VM, digite o comando 

```
python3 /home/ricardo_semerene/etl_prod/main.py
```
Para interromper o código manualmente, no mesmo lugar que se digita o comando acima, execute o comando:
```
ps aux | grep main.py
```
Vai aparecer um número do processo que está sendo executado. Desse modo, basta digitar um último comando:

```
kill <number>
```



