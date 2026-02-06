import random
import os
import sys
from datetime import datetime

# Dependency Check
try:
    import customtkinter as ctk
except ImportError:
    print("Error: 'customtkinter' not found. Install it with: pip install customtkinter")
    sys.exit(1)

try:
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
except ImportError:
    print("Error: 'reportlab' not found. Install it with: pip install reportlab")
    sys.exit(1)

def get_turn():
    val = random.randint(0, 6)
    sign = random.choice(["+", "-"]) if val != 0 else "+"
    return f"{val}{sign}"

def generate_scramble():
    p1 = ["DR", "DL", "U", "R", "D", "L", "ALL"]
    p2 = ["R", "D", "L", "ALL"]
    part1 = " ".join([f"{m}{get_turn()}" for m in p1])
    part2 = " ".join([f"{m}{get_turn()}" for m in p2])
    return f"{part1} y2 {part2}"

class TriangleClockScrambler(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("TriangleClockScrambler v1.0")
        self.geometry("460x400")
        ctk.set_appearance_mode("dark")
        
        # UI Setup
        self.header = ctk.CTkLabel(self, text="Triangle Clock", font=("Helvetica", 28, "bold"))
        self.header.pack(pady=(20, 5))
        
        self.subheader = ctk.CTkLabel(self, text="Official Scramble Generator", text_color="gray")
        self.subheader.pack(pady=(0, 20))

        self.event_name = ctk.CTkEntry(self, placeholder_text="Event Name", width=320)
        self.event_name.insert(0, "Greenwoods Clock Clash 2026")
        self.event_name.pack(pady=10)

        self.count_val = ctk.CTkEntry(self, placeholder_text="Number of Scrambles", width=320)
        self.count_val.insert(0, "5")
        self.count_val.pack(pady=10)

        self.btn = ctk.CTkButton(self, text="GENERATE PDF", command=self.run_logic, 
                                 font=("Helvetica", 14, "bold"), height=45, width=220,
                                 fg_color="#2ecc71", hover_color="#27ae60")
        self.btn.pack(pady=30)

        self.status = ctk.CTkLabel(self, text="Ready to Scramble", text_color="gray")
        self.status.pack()

    def run_logic(self):
        try:
            name = self.event_name.get()
            count = int(self.count_val.get())
            
            # EXE-safe path finding
            if getattr(sys, 'frozen', False):
                application_path = os.path.dirname(sys.executable)
            else:
                application_path = os.path.dirname(os.path.abspath(__file__))

            timestamp = datetime.now().strftime('%H%M%S')
            filename = os.path.join(application_path, f"Triangle_Scrambles_{timestamp}.pdf")
            
            self.build_pdf(name, count, filename)
            self.status.configure(text=f"PDF Saved Successfully!", text_color="#2ecc71")
        except ValueError:
            self.status.configure(text="Error: Scramble count must be a number", text_color="#e74c3c")
        except Exception as e:
            self.status.configure(text=f"Error: {str(e)}", text_color="#e74c3c")

    def build_pdf(self, event, count, path):
        doc = SimpleDocTemplate(path, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
        styles = getSampleStyleSheet()
        mono = ParagraphStyle('Mono', fontName='Courier', fontSize=10, leading=14)
        
        mains = [generate_scramble() for _ in range(count)]
        extras = [generate_scramble() for _ in range(2)]

        elements = [
            Paragraph("Triangle Clock Scrambles", styles['Heading1']),
            Paragraph(f"Competition: {event}", styles['Heading2']),
            Paragraph(f"Date/Time Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['Normal']),
            Spacer(1, 20)
        ]

        data = [["#", "Scramble String"]]
        for i, s in enumerate(mains, 1):
            data.append([str(i), Paragraph(s, mono)])
        for i, s in enumerate(extras, 1):
            data.append([f"E{i}", Paragraph(s, mono)])

        t = Table(data, colWidths=[35, 470])
        t.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('TOPPADDING', (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ]))
        
        elements.append(t)
        doc.build(elements)

if __name__ == "__main__":
    app = TriangleClockScrambler()
    app.mainloop()