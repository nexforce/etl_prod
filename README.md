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
