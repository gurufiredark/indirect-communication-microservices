const express = require('express');
const { MongoClient } = require('mongodb');
const { promisify } = require('util');
const amqp = require('amqplib/callback_api');

const app = express();
const port = 4000;
let channel;
let db;

app.use(express.json());

async function initializeDatabase() {
  const compra = new MongoClient('mongodb+srv://gabriel:118038@trabsd.bivozhe.mongodb.net/?retryWrites=true&w=majority', 
  { useNewUrlParser: true, useUnifiedTopology: true });

  try {
    await compra.connect();
    db = compra.db('mydb');
    console.log('Conectado ao banco de dados MongoDB.');
  } catch (error) {
    console.error('Erro ao conectar ao banco de dados MongoDB:', error);
    throw error;
  }
}

// Conectar ao RabbitMQ
const amqpConnect = promisify(amqp.connect);

async function connectToRabbitMQ() {
  try {
    const connection = await amqpConnect('amqp://localhost');
    channel = await connection.createChannel();
    const estoqueQueue = 'compraQueue';

    channel.assertQueue(estoqueQueue, { durable: false });
    
    console.log('Aguardando mensagens de estoque...');

    channel.consume(estoqueQueue, async (msg) => {
      const { id, nome, quantidade } = JSON.parse(msg.content.toString());

      // Processar a compra e salvar no MongoDB
      const Compra = db.collection('compras'); 
      try {
        await Compra.insertOne({ id, nome, quantidade });
        console.log(`Compra registrada: ${id} ${nome} ${quantidade}`);
      } catch (error) {
        console.error(`Erro ao registrar compra: ${error}`);
      }
    }, {
      noAck: true
    });
  } catch (error) {
    console.error('Erro ao conectar ao RabbitMQ:', error);
    throw error;
  }
}

// Rota para realizar uma compra
app.post('/compra', async (req, res) => {
  const { id, quantidade } = req.body;

  // Verificar se o produto com o ID existe no banco de dados
  const produtoExistente = await db.collection('produtos').findOne({ id: id });

  if (!produtoExistente) {
    return res.status(404).json({ mensagem: 'Produto com o ID não encontrado.' });
  }

  // Envie uma mensagem para a fila do RabbitMQ
  channel.sendToQueue('compraQueue', Buffer.from(JSON.stringify({ id, quantidade })));
  res.status(200).json({ mensagem: 'Pedido de compra recebido com sucesso.' });
});

// Inicializar o servidor
Promise.all([initializeDatabase(), connectToRabbitMQ()]).then(() => {
  app.listen(port, () => {
    console.log(`Microserviço de compra rodando em http://localhost:${port}/compra`);
  });
});
