const amqp = require('amqplib');
const express = require('express');
const { MongoClient } = require('mongodb');

const app = express();
const port = 4001;
let db;

app.use(express.json());

async function initializeDatabase() {
  const produtos = new MongoClient('mongodb+srv://gabriel:118038@trabsd.bivozhe.mongodb.net/?retryWrites=true&w=majority', 
  { useNewUrlParser: true, useUnifiedTopology: true });

  try {
    await produtos.connect();
    db = produtos.db('mydb');
    console.log('Conectado ao banco de dados MongoDB.');
  } catch (error) {
    console.error('Erro ao conectar ao banco de dados MongoDB:', error);
    throw error;
  }
}

app.post('/registrar-produto', async (req, res) => {
  const { id, nome, preco, quantidade } = req.body;

  // Verificar se o produto com o mesmo ID já existe no banco de dados
  const produtoExistente = await db.collection('produtos').findOne({ id: id });

  if (produtoExistente) {
    return res.status(400).json({ message: 'Produto com ID já existe.' });
  }

  // Insere o produto no banco de dados
  const resultado = await db.collection('produtos').insertOne({ id, nome, preco, quantidade });
  
  res.json({ message: `Produto: ${nome} registrado com sucesso.`, insertedCount: resultado.insertedCount });
});

async function processarMensagemCompra(mensagem) {
  try {
    const { id, nome, quantidade } = JSON.parse(mensagem.content.toString());

    // Verificar se o produto existe no estoque
    const produtoNoEstoque = await db.collection('produtos').findOne({ id: id });

    if (!produtoNoEstoque) {
      console.log(`Produto ${produtoNoEstoque.nome} não encontrado no estoque.`);
      return;
    }

    // Verificar se há quantidade suficiente em estoque
    if (produtoNoEstoque.quantidade < quantidade) {
      console.log(`Estoque insuficiente para o produto ${produtoNoEstoque.nome}.`);
      return;
    }

    // Atualizar a quantidade em estoque no MongoDB
    await db.collection('produtos').updateOne(
      { id: id },
      { $inc: { quantidade: -quantidade } }
    );

    console.log(`Estoque atualizado para ${produtoNoEstoque.quantidade - quantidade} unidades do produto ${produtoNoEstoque.nome}`);
    
    // Lógica de processamento da mensagem de compra
    console.log(`Processando mensagem de compra para o produto ${produtoNoEstoque.nome}`);
  } catch (error) {
    console.error('Erro ao processar mensagem de compra:', error);
  }
}

// conexão com o RabbitMQ
async function start() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const compraQueue = 'compraQueue';

  await channel.assertQueue(compraQueue, { durable: false });
  console.log(`Aguardando mensagens em ${compraQueue}`);

  channel.consume(compraQueue, processarMensagemCompra, { noAck: true });
}

initializeDatabase().then(() => {
  start();
  app.listen(port, () => {
    console.log(`Serviço de Registro de Produtos rodando em http://localhost:${port}`);
  });
});
