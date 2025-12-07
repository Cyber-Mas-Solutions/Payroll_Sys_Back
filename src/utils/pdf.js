const PDFDocument = require('pdfkit');

function employeeListPdf(employees, res) {
  const doc = new PDFDocument({ margin: 36, size: 'A4' });

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', 'inline; filename="employees.pdf"');

  doc.pipe(res);

  doc.fontSize(18).text('Employee Information Report', { align: 'center' });
  doc.moveDown();

  const headers = ['Code', 'Name', 'Department', 'Designation', 'Status', 'Join Date'];
  doc.fontSize(12).text(headers.join(' | '));
  doc.moveDown(0.5);

  employees.forEach(e => {
    doc.text([
      e.employee_code || '',
      e.full_name,
      e.department_name || '',
      e.designation || '',
      e.status || '',
      e.joining_date ? new Date(e.joining_date).toISOString().slice(0,10) : ''
    ].join(' | '));
  });

  doc.end();
}

module.exports = { employeeListPdf };
