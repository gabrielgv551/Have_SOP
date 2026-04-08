module.exports = (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.json({
    status: 'ok',
    project: 'have-gestor-api-2',
    timestamp: new Date().toISOString(),
  });
};
