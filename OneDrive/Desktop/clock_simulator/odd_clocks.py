import customtkinter as ctk
import tkinter as tk
import math
import random
import time

class SimulatorClock(tk.Canvas):
    def __init__(self, master, app, size=750, is_front=True, **kwargs):
        super().__init__(master, width=size, height=size, bg="#ffffff", highlightthickness=0, **kwargs)
        self.app = app
        self.size, self.center, self.is_front = size, size / 2, is_front
        self.clock_values = []
        self.update_theme()

    def update_theme(self):
        self.body_color = "#e8f5e9" if self.is_front else "#2e7d32"
        self.face_color = "#2e7d32" if self.is_front else "#e8f5e9"
        self.ptr_color = "#a5d6a7" if self.is_front else "#1b5e20" 
        self.pin_up_color = "#FFB74D"
        self.pin_down_color = "#E65100"
        self.marker_dot_color = "#4A148C" if self.is_front else "#E1BEE7" 

    def draw_pointer(self, cx, cy, radius, value):
        angle_rad = math.radians(value * 30 - 90)
        tip_x, tip_y = cx + radius * math.cos(angle_rad), cy + radius * math.sin(angle_rad)
        base_w, angle_deg = 11, value * 30 - 90
        l_x = cx + base_w * math.cos(math.radians(angle_deg - 110))
        l_y = cy + base_w * math.sin(math.radians(angle_deg - 110))
        r_x = cx + base_w * math.cos(math.radians(angle_deg + 110))
        r_y = cy + base_w * math.sin(math.radians(angle_deg + 110))
        self.create_polygon(l_x, l_y, tip_x, tip_y, r_x, r_y, fill=self.ptr_color, outline="#000000", width=1.5, smooth=True, tags="pointer")
        self.create_oval(cx-4, cy-4, cx+4, cy+4, fill="#ffffff", outline="#000000", tags="pointer")

    def draw_external_buttons(self, cx, cy, radius, corner_key, btn_r=15, font_size=14, offset=60):
        side_prefix = "f_" if self.is_front else "b_"
        for label, angle, delta in [("-", math.radians(-140), -1), ("+", math.radians(-40), 1)]:
            bx, by = cx + offset * math.cos(angle), cy + offset * math.sin(angle)
            tag = f"{side_prefix}btn_{corner_key}_{label}"
            self.create_oval(bx-20, by-20, bx+20, by+20, fill="", outline="", tags=(tag, "btn"))
            self.create_oval(bx-btn_r, by-btn_r, bx+btn_r, by+btn_r, fill="#ffffff", outline="#000000", width=2, tags=(tag, "btn"))
            self.create_text(bx, by, text=label, fill="#000000", font=("Arial", font_size, "bold"), tags=(tag, "btn"))
            self.tag_bind(tag, "<Button-1>", lambda e, d=delta, k=corner_key: self.app.rotate_official(k, d, self.is_front))

    def render_puzzle(self):
        self.delete("all")
        if not self.clock_values: return
        mode = self.app.mode_var.get()
        side_text = "FRONT FACE" if self.is_front else "BACK FACE"
        label_color = "#2e7d32" if self.is_front else "#e8f5e9"
        self.create_text(self.center, 30, text=side_text, fill=label_color, font=("Arial", 24, "bold"))

        if mode == "Triangular":
            clock_r, marker_r, hull_r, lobe_hull_r = 52, 60, 210, 80
            lobe_pos = [(0, -180), (-180, 130), (180, 130)]
            self.pin_coords = [(0, -90), (-100, 75), (100, 75)]
            self.pin_labels = ["U", "L", "R"]
            self.pin_radius, self.clock_pos = 18, [(0, -180), (-110, -35), (110, -35), (-180, 130), (0, 130), (180, 130)]
            self.corner_map = {"U": 0, "L": 3, "R": 5} if self.is_front else {"U": 0, "L": 5, "R": 3}
        elif mode == "3x3":
            clock_r, marker_r, hull_r, lobe_hull_r, spacing = 45, 52, 215, 55, 130
            self.clock_pos = [((c-1)*spacing, (r-1)*spacing) for r in range(3) for c in range(3)]
            lobe_pos = [(-145, -145), (145, -145), (-145, 145), (145, 145)]
            self.pin_coords = [((c-0.5)*spacing, (r-0.5)*spacing) for r in range(2) for c in range(2)]
            self.pin_labels = [f"P{i+1}" for i in range(4)]
            self.pin_radius, self.corner_map = 15, {"C1": 0, "C2": 2, "C3": 6, "C4": 8}
        elif mode == "4x4":
            clock_r, marker_r, hull_r, lobe_hull_r, spacing = 35, 42, 230, 70, 100
            self.clock_pos = [( (c-1.5)*spacing, (r-1.5)*spacing ) for r in range(4) for c in range(4)]
            lobe_pos = [(-140, -140), (140, -140), (-140, 140), (140, 140)]
            self.pin_coords = [( (c-1)*spacing, (r-1)*spacing ) for r in range(3) for c in range(3)]
            self.pin_labels = [f"P{i+1}" for i in range(9)]
            self.pin_radius, self.corner_map = 12, {"C1": 0, "C2": 3, "C3": 12, "C4": 15}
        elif mode == "5x5":
            clock_r, marker_r, hull_r, lobe_hull_r, spacing = 30, 36, 260, 60, 90
            self.clock_pos = [( (c-2)*spacing, (r-2)*spacing ) for r in range(5) for c in range(5)]
            lobe_pos = [(-180, -180), (180, -180), (-180, 180), (180, 180)]
            self.pin_coords = [( (c-1.5)*spacing, (r-1.5)*spacing ) for r in range(4) for c in range(4)]
            self.pin_labels = [f"P{i+1}" for i in range(16)]
            self.pin_radius, self.corner_map = 10, {"C1": 0, "C2": 4, "C3": 20, "C4": 24}
        elif mode == "6x6":
            clock_r, marker_r, hull_r, lobe_hull_r, spacing = 26, 31, 280, 50, 78
            self.clock_pos = [( (c-2.5)*spacing, (r-2.5)*spacing ) for r in range(6) for c in range(6)]
            lobe_pos = [(-200, -200), (200, -200), (-200, 200), (200, 200)]
            self.pin_coords = [( (c-2)*spacing, (r-2)*spacing ) for r in range(5) for c in range(5)]
            self.pin_labels = [f"P{i+1}" for i in range(25)]
            self.pin_radius, self.corner_map = 9, {"C1": 0, "C2": 5, "C3": 30, "C4": 35}
        elif mode == "7x7":
            clock_r, marker_r, hull_r, lobe_hull_r, spacing = 22, 27, 305, 45, 68
            self.clock_pos = [( (c-3)*spacing, (r-3)*spacing ) for r in range(7) for c in range(7)]
            lobe_pos = [(-220, -220), (220, -220), (-220, 220), (220, 220)]
            self.pin_coords = [( (c-2.5)*spacing, (r-2.5)*spacing ) for r in range(6) for c in range(6)]
            self.pin_labels = [f"P{i+1}" for i in range(36)]
            self.pin_radius, self.corner_map = 8, {"C1": 0, "C2": 6, "C3": 42, "C4": 48}
        else:
            clock_r, marker_r, hull_r, lobe_hull_r = 40, 48, 225, 65
            lobe_dist, self.pin_radius = 195, 15
            lobe_pos = [(lobe_dist * math.cos(math.radians(i * 72 - 90)), lobe_dist * math.sin(math.radians(i * 72 - 90))) for i in range(5)]
            p_order = [0, 1, 2, 3, 4] if self.is_front else [0, 4, 3, 2, 1]
            self.pin_coords = [(115 * math.cos(math.radians(i * 72 - 90)), 115 * math.sin(math.radians(i * 72 - 90))) for i in p_order]
            self.pin_labels = ["FU", "FUR", "FDR", "FDL", "FUL"] if self.is_front else ["BU", "BUL", "BDL", "BDR", "BUR"]
            self.clock_pos = []
            for i in range(5):
                a = math.radians(i * 72 - 90); self.clock_pos.append((190 * math.cos(a), 190 * math.sin(a)))
            for i in range(5):
                a = math.radians(i * 72 - 54); self.clock_pos.append((145 * math.cos(a), 145 * math.sin(a)))
            if mode == "Super-Pentagonal": self.clock_pos.append((0, -45))
            self.corner_map = {self.pin_labels[i]: i for i in range(min(5, len(self.pin_labels)))}

        pts = []
        for angle in range(0, 360, 2):
            rad = math.radians(angle)
            max_d = hull_r
            centers = [(0, 0, hull_r)] + [(pos[0], pos[1], lobe_hull_r) for pos in lobe_pos]
            for cx, cy, r in centers:
                bv = -2 * (cx * math.cos(rad) + cy * math.sin(rad))
                cv = cx**2 + cy**2 - r**2
                det = bv**2 - 4 * cv
                if det >= 0:
                    d = (-bv + math.sqrt(det)) / 2
                    max_d = max(max_d, d)
            pts.extend([self.center + max_d * math.cos(rad), self.center + max_d * math.sin(rad)])
        self.create_polygon(pts, fill=self.body_color, outline="#000000", width=3, smooth=True, tags="body")

        prefix = "F" if self.is_front else "B"
        for i, (dx, dy) in enumerate(self.clock_pos):
            if i >= len(self.clock_values): break
            cx, cy = self.center + dx, self.center + dy
            val = self.clock_values[i]
            self.create_oval(cx-clock_r, cy-clock_r, cx+clock_r, cy+clock_r, fill=self.face_color, outline="#000000", width=1.5, tags="clock")
            text_fill = "#ffffff" if self.is_front else "#2e7d32"
            self.create_text(cx, cy + (clock_r * 0.5), text=f"{prefix}{i+1}", fill=text_fill, font=("Arial", 10, "bold"))
            for h in range(12):
                a = math.radians(h * 30 - 90)
                mx, my = cx + marker_r * math.cos(a), cy + marker_r * math.sin(a)
                if h == 0: self.create_text(mx, my, text="II", fill="#ff0000", font=("Arial", 10, "bold"))
                else: self.create_oval(mx-1.5, my-1.5, mx+1.5, my+1.5, fill=self.marker_dot_color, outline="")
            self.draw_pointer(cx, cy, clock_r, val)
            
            if mode == "Triangular":
                for key, idx in self.corner_map.items():
                    if i == idx: self.draw_external_buttons(cx, cy, clock_r, key, offset=60)
            elif "Grid" in mode or "x" in mode:
                if i in self.corner_map.values():
                    ckey = [k for k,v in self.corner_map.items() if v == i][0]
                    b_size = 11 if mode == "3x3" else 8
                    f_size = 11 if mode == "3x3" else 8
                    btn_offset = 48 if mode == "3x3" else 38
                    self.draw_external_buttons(cx, cy, clock_r, ckey, btn_r=b_size, font_size=f_size, offset=btn_offset)
            elif i < 5:
                self.draw_external_buttons(cx, cy, clock_r, self.pin_labels[i], offset=60)

        for i, (px, py) in enumerate(self.pin_coords):
            pcx, pcy = self.center + px, self.center + py
            if mode == "3x3": mirror_map = {0:1, 1:0, 2:3, 3:2}
            elif mode == "4x4": mirror_map = {0:2, 1:1, 2:0, 3:5, 4:4, 5:3, 6:8, 7:7, 8:6}
            elif mode == "5x5": mirror_map = {i: ((i // 4) * 4 + (3 - (i % 4))) for i in range(16)}
            elif mode == "6x6": mirror_map = {i: ((i // 5) * 5 + (4 - (i % 5))) for i in range(25)}
            elif mode == "7x7": mirror_map = {i: ((i // 6) * 6 + (5 - (i % 6))) for i in range(36)}
            else: mirror_map = None
            
            if mirror_map: actual_idx = i if self.is_front else mirror_map[i]
            else: actual_idx = i if self.is_front else ([0, 2, 1][i] if mode == "Triangular" else [0, 1, 2, 3, 4][i])
            is_up = self.app.pin_states[actual_idx] if self.is_front else not self.app.pin_states[actual_idx]
            fill = self.pin_up_color if is_up else self.pin_down_color
            p_circ = self.create_oval(pcx-self.pin_radius, pcy-self.pin_radius, pcx+self.pin_radius, pcy+self.pin_radius, fill=fill, outline="#000000", width=2, tags="pin")
            self.tag_bind(p_circ, "<Button-1>", lambda e, idx=actual_idx: self.toggle_pin(idx))

        self.tag_raise("pin"); self.tag_raise("btn")

    def toggle_pin(self, idx):
        self.app.start_timer_logic(); self.app.pin_states[idx] = not self.app.pin_states[idx]; self.app.update_all()

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Crazy Clock Simulator")
        self.geometry("1600x950"); self.configure(fg_color="#ffffff")
        self.is_waiting_for_first_move = self.timer_running = self.is_scrambling = False
        self.modes = ["Triangular", "3x3", "Pentagonal", "Super-Pentagonal", "4x4", "5x5", "6x6", "7x7"]
        self.high_scores = {m: None for m in self.modes}
        self.mode_var = ctk.StringVar(value="Triangular")
        self.pin_states = [True, True, True]
        self.container = ctk.CTkFrame(self, fg_color="#ffffff")
        self.container.pack(expand=True, pady=10)
        self.front = SimulatorClock(self.container, self, is_front=True)
        self.back = SimulatorClock(self.container, self, is_front=False)
        self.front.grid(row=0, column=0, padx=20); self.back.grid(row=0, column=1, padx=20)
        ctrl = ctk.CTkFrame(self, fg_color="#f1f8e9", corner_radius=15)
        ctrl.pack(fill="x", side="bottom", padx=40, pady=20)
        ctk.CTkOptionMenu(ctrl, values=self.modes, command=self.change_mode, variable=self.mode_var, fg_color="#2e7d32").pack(side="left", padx=20, pady=15)
        ctk.CTkButton(ctrl, text="SCRAMBLE", command=self.scramble, fg_color="#ff4757").pack(side="left", padx=10)
        ctk.CTkButton(ctrl, text="RESET", command=self.reset, fg_color="#2d3436").pack(side="left", padx=10)
        score_panel = ctk.CTkFrame(ctrl, fg_color="#000000", corner_radius=10)
        score_panel.pack(side="right", padx=30, pady=10)
        self.high_score_label = ctk.CTkLabel(score_panel, text="Best: --:--.--", font=("Courier", 16, "bold"), text_color="#E1BEE7")
        self.high_score_label.pack(padx=10, pady=(5, 0))
        self.timer_label = ctk.CTkLabel(score_panel, text="00:00.00", font=("Courier", 32, "bold"), text_color="#ff007f", width=200)
        self.timer_label.pack(padx=10, pady=(0, 5))
        self.change_mode("Triangular")

    def format_time(self, seconds):
        if seconds is None: return "--:--.--"
        mins, secs = divmod(seconds, 60)
        return f"{int(mins):02}:{secs:05.2f}"

    def start_timer_logic(self):
        if not self.is_scrambling and self.is_waiting_for_first_move and not self.timer_running:
            self.is_waiting_for_first_move, self.timer_running, self.start_time = False, True, time.time()
            self.run_timer_loop()

    def run_timer_loop(self):
        if self.timer_running:
            self.timer_label.configure(text=self.format_time(time.time() - self.start_time))
            self.after(30, self.run_timer_loop)

    def check_solved(self):
        if not self.timer_running: return
        if all(v == 12 for v in self.front.clock_values) and all(v == 12 for v in self.back.clock_values):
            self.timer_running = False
            final_time = time.time() - self.start_time
            best = self.high_scores[self.mode_var.get()]
            if best is None or final_time < best:
                self.high_scores[self.mode_var.get()] = final_time
                self.update_score_label()

    def update_score_label(self):
        best = self.high_scores[self.mode_var.get()]
        self.high_score_label.configure(text=f"Best: {self.format_time(best)}")

    def change_mode(self, mode):
        self.timer_running = self.is_waiting_for_first_move = False
        self.timer_label.configure(text="00:00.00"); self.update_score_label()
        p_count = 3 if mode == "Triangular" else (4 if mode == "3x3" else (9 if mode == "4x4" else (16 if mode == "5x5" else (25 if mode=="6x6" else (36 if mode=="7x7" else 5)))))
        c_count = 6 if mode == "Triangular" else (9 if mode == "3x3" else (16 if mode == "4x4" else (25 if mode == "5x5" else (36 if mode=="6x6" else (49 if mode=="7x7" else (11 if mode=="Super-Pentagonal" else 10))))))
        self.pin_states, self.front.clock_values, self.back.clock_values = [True]*p_count, [12]*c_count, [12]*c_count
        self.update_all()

    def reset(self): self.change_mode(self.mode_var.get())
    def update_all(self): self.front.render_puzzle(); self.back.render_puzzle(); self.check_solved()

    def rotate_official(self, move_key, delta, is_front_click):
        self.start_timer_logic()
        mode = self.mode_var.get()
        if mode == "Triangular": self.rotate_triangular(move_key, delta, is_front_click)
        elif mode == "3x3": self.rotate_3x3(move_key, delta, is_front_click)
        elif mode == "4x4": self.rotate_4x4(move_key, delta, is_front_click)
        elif mode == "5x5": self.rotate_5x5(move_key, delta, is_front_click)
        elif mode == "6x6": self.rotate_6x6(move_key, delta, is_front_click)
        elif mode == "7x7": self.rotate_7x7(move_key, delta, is_front_click)
        else: self.rotate_pentagonal(move_key, delta, is_front_click)

    def apply_move(self, f_set, b_set, delta, is_front_click):
        f_delta, b_delta = (delta, -delta) if is_front_click else (-delta, delta)
        for idx in f_set: self.front.clock_values[idx-1] = (self.front.clock_values[idx-1] + f_delta - 1) % 12 + 1
        for idx in b_set: self.back.clock_values[idx-1] = (self.back.clock_values[idx-1] + b_delta - 1) % 12 + 1
        self.update_all()

    def rotate_3x3(self, move_key, delta, is_front_click):
        p = self.pin_states; f_idx, b_idx = set(), set()
        mirror_pin = {0:1, 1:0, 2:3, 3:2}
        pin_logic = {0: {"active": [1, 2, 4, 5], "passive_corner": [3]}, 1: {"active": [2, 3, 5, 6], "passive_corner": [1]},
                     2: {"active": [4, 5, 7, 8], "passive_corner": [9]}, 3: {"active": [5, 6, 8, 9], "passive_corner": [7]}}
        target_pin = {"C1":0, "C2":1, "C3":2, "C4":3}[move_key]
        actual_master_idx = target_pin if is_front_click else mirror_pin[target_pin]
        master_is_up = p[actual_master_idx] if is_front_click else not p[actual_master_idx]
        for pos_idx in range(4):
            phys_idx = pos_idx if is_front_click else mirror_pin[pos_idx]
            is_up = p[phys_idx] if is_front_click else not p[phys_idx]
            if is_up == master_is_up:
                logic = pin_logic[pos_idx if is_up else mirror_pin[pos_idx]]
                if (is_up and is_front_click) or (not is_up and not is_front_click): f_idx.update(logic["active"]); b_idx.update(logic["passive_corner"])
                else: b_idx.update(logic["active"]); f_idx.update(logic["passive_corner"])
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def rotate_4x4(self, move_key, delta, is_front_click):
        p = self.pin_states; f_idx, b_idx = set(), set()
        mirror_pin = {0:2, 1:1, 2:0, 3:5, 4:4, 5:3, 6:8, 7:7, 8:6}
        key_to_idx = {"C1": 0, "C2": 2, "C3": 6, "C4": 8}
        master_pos_idx = key_to_idx.get(move_key, 0)
        actual_master_idx = master_pos_idx if is_front_click else mirror_pin[master_pos_idx]
        master_is_up = p[actual_master_idx] if is_front_click else not p[actual_master_idx]
        pin_logic = {
            0: {"active": [1, 2, 5, 6], "passive_corner": [4]}, 1: {"active": [2, 3, 6, 7], "passive_corner": []},
            2: {"active": [3, 4, 7, 8], "passive_corner": [1]}, 3: {"active": [5, 6, 9, 10], "passive_corner": []},
            4: {"active": [6, 7, 10, 11], "passive_corner": []}, 5: {"active": [7, 8, 11, 12], "passive_corner": []},
            6: {"active": [9, 10, 13, 14], "passive_corner": [16]}, 7: {"active": [10, 11, 14, 15], "passive_corner": []},
            8: {"active": [11, 12, 15, 16], "passive_corner": [13]}
        }
        for pos_idx in range(9):
            phys_idx = pos_idx if is_front_click else mirror_pin[pos_idx]
            is_up = p[phys_idx] if is_front_click else not p[phys_idx]
            if is_up == master_is_up:
                logic = pin_logic[pos_idx if is_up else mirror_pin[pos_idx]]
                if (is_up and is_front_click) or (not is_up and not is_front_click): f_idx.update(logic["active"]); b_idx.update(logic["passive_corner"])
                else: b_idx.update(logic["active"]); f_idx.update(logic["passive_corner"])
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def rotate_5x5(self, move_key, delta, is_front_click):
        p = self.pin_states; f_idx, b_idx = set(), set()
        mirror_pin = {i: ((i // 4) * 4 + (3 - (i % 4))) for i in range(16)}
        key_to_idx = {"C1": 0, "C2": 3, "C3": 12, "C4": 15}
        master_pos_idx = key_to_idx.get(move_key, 0)
        actual_master_idx = master_pos_idx if is_front_click else mirror_pin[master_pos_idx]
        master_is_up = p[actual_master_idx] if is_front_click else not p[actual_master_idx]
        pin_logic = {}
        for i in range(16):
            r, c = divmod(i, 4); active = [(r*5)+c+1, (r*5)+c+2, ((r+1)*5)+c+1, ((r+1)*5)+c+2]
            passive = [5] if i == 0 else ([1] if i == 3 else ([25] if i == 12 else ([21] if i == 15 else [])))
            pin_logic[i] = {"active": active, "passive_corner": passive}
        for pos_idx in range(16):
            phys_idx = pos_idx if is_front_click else mirror_pin[pos_idx]
            is_up = p[phys_idx] if is_front_click else not p[phys_idx]
            if is_up == master_is_up:
                logic = pin_logic[pos_idx if is_up else mirror_pin[pos_idx]]
                if (is_up and is_front_click) or (not is_up and not is_front_click): f_idx.update(logic["active"]); b_idx.update(logic["passive_corner"])
                else: b_idx.update(logic["active"]); f_idx.update(logic["passive_corner"])
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def rotate_6x6(self, move_key, delta, is_front_click):
        p = self.pin_states; f_idx, b_idx = set(), set()
        mirror_pin = {i: ((i // 5) * 5 + (4 - (i % 5))) for i in range(25)}
        key_to_idx = {"C1": 0, "C2": 4, "C3": 20, "C4": 24}
        master_pos_idx = key_to_idx.get(move_key, 0)
        actual_master_idx = master_pos_idx if is_front_click else mirror_pin[master_pos_idx]
        master_is_up = p[actual_master_idx] if is_front_click else not p[actual_master_idx]
        pin_logic = {}
        for i in range(25):
            r, c = divmod(i, 5); active = [(r*6)+c+1, (r*6)+c+2, ((r+1)*6)+c+1, ((r+1)*6)+c+2]
            passive = [6] if i == 0 else ([1] if i == 4 else ([36] if i == 20 else ([31] if i == 24 else [])))
            pin_logic[i] = {"active": active, "passive_corner": passive}
        for pos_idx in range(25):
            phys_idx = pos_idx if is_front_click else mirror_pin[pos_idx]
            is_up = p[phys_idx] if is_front_click else not p[phys_idx]
            if is_up == master_is_up:
                logic = pin_logic[pos_idx if is_up else mirror_pin[pos_idx]]
                if (is_up and is_front_click) or (not is_up and not is_front_click): f_idx.update(logic["active"]); b_idx.update(logic["passive_corner"])
                else: b_idx.update(logic["active"]); f_idx.update(logic["passive_corner"])
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def rotate_7x7(self, move_key, delta, is_front_click):
        p = self.pin_states; f_idx, b_idx = set(), set()
        mirror_pin = {i: ((i // 6) * 6 + (5 - (i % 6))) for i in range(36)}
        key_to_idx = {"C1": 0, "C2": 5, "C3": 30, "C4": 35}
        master_pos_idx = key_to_idx.get(move_key, 0)
        actual_master_idx = master_pos_idx if is_front_click else mirror_pin[master_pos_idx]
        master_is_up = p[actual_master_idx] if is_front_click else not p[actual_master_idx]
        pin_logic = {}
        for i in range(36):
            r, c = divmod(i, 6); active = [(r*7)+c+1, (r*7)+c+2, ((r+1)*7)+c+1, ((r+1)*7)+c+2]
            passive = [7] if i == 0 else ([1] if i == 5 else ([49] if i == 30 else ([43] if i == 35 else [])))
            pin_logic[i] = {"active": active, "passive_corner": passive}
        for pos_idx in range(36):
            phys_idx = pos_idx if is_front_click else mirror_pin[pos_idx]
            is_up = p[phys_idx] if is_front_click else not p[phys_idx]
            if is_up == master_is_up:
                logic = pin_logic[pos_idx if is_up else mirror_pin[pos_idx]]
                if (is_up and is_front_click) or (not is_up and not is_front_click): f_idx.update(logic["active"]); b_idx.update(logic["passive_corner"])
                else: b_idx.update(logic["active"]); f_idx.update(logic["passive_corner"])
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def rotate_triangular(self, move_key, delta, is_front_click):
        u, l, r = self.pin_states; f_idx, b_idx = set(), set()
        pins = {'U': u, 'L': l, 'R': r} if is_front_click else {'U': not u, 'L': not l, 'R': not r}
        master_state = pins[move_key]
        if is_front_click:
            if u == master_state: (f_idx.update([1, 2, 3]), b_idx.add(1)) if u else (f_idx.add(1), b_idx.update([1, 2, 3]))
            if l == master_state: (f_idx.update([4, 2, 5]), b_idx.add(6)) if l else (f_idx.add(4), b_idx.update([5, 6, 3]))
            if r == master_state: (f_idx.update([5, 6, 3]), b_idx.add(4)) if r else (f_idx.add(6), b_idx.update([4, 2, 5]))
        else:
            if (not u) == master_state: (b_idx.update([1, 2, 3]), f_idx.add(1)) if not u else (b_idx.add(1), f_idx.update([1, 2, 3]))
            if (not r) == master_state: (b_idx.update([2, 4, 5]), f_idx.add(6)) if not r else (b_idx.add(4), f_idx.update([3, 5, 6]))
            if (not l) == master_state: (b_idx.update([3, 5, 6]), f_idx.add(4)) if not l else (b_idx.add(6), f_idx.update([2, 4, 5]))
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def rotate_pentagonal(self, move_key, delta, is_front_click):
        p = self.pin_states; f_idx, b_idx = set(), set()
        logic_map = {"FU":0, "FUR":1, "FDR":2, "FDL":3, "FUL":4, "BU":0, "BUR":1, "BDR":2, "BDL":3, "BUL":4}
        pins = {k: (p[v] if is_front_click else not p[v]) for k, v in logic_map.items()}
        master_state = pins[move_key]
        back_rules = {0:{"f":{1},"b":{1,6,10,11}}, 4:{"f":{5},"b":{2,6,7}}, 3:{"f":{4},"b":{3,7,8}}, 2:{"f":{3},"b":{4,8,9}}, 1:{"f":{2},"b":{5,9,10}}}
        front_rules = {0:{"f":{1,6,10,11},"b":{1}}, 1:{"f":{2,6,7},"b":{5}}, 2:{"f":{3,7,8},"b":{4}}, 3:{"f":{4,8,9},"b":{3}}, 4:{"f":{5,9,10},"b":{2}}}
        for i in range(5):
            cur_p = p[i] if is_front_click else not p[i]
            if cur_p == master_state:
                rules = (front_rules if is_front_click else back_rules) if cur_p else (back_rules if is_front_click else front_rules)
                f_idx.update(rules[i]["f"]); b_idx.update(rules[i]["b"])
        self.apply_move(f_idx, b_idx, delta, is_front_click)

    def scramble(self):
        self.timer_running, self.is_waiting_for_first_move, self.is_scrambling = False, True, True
        self.timer_label.configure(text="00:00.00")
        mode = self.mode_var.get()
        
        # Determine movement keys based on mode
        if "x" in mode or "Grid" in mode:
            f_keys = ["C1", "C2", "C3", "C4"]
        elif "Pentagonal" in mode:
            f_keys = ["FU", "FUR", "FDR", "FDL", "FUL"]
        else:
            f_keys = ["U", "L", "R"]

        # Calculate dynamic scramble depth: Clocks * 5 (Min 30 moves)
        clock_count = len(self.front.clock_values)
        scramble_depth = max(30, clock_count * 5)

        for _ in range(scramble_depth):
            # Randomize pins before each turn to simulate real hand movements
            self.pin_states = [random.choice([True, False]) for _ in range(len(self.pin_states))]
            
            # Perform a random rotation (1-11 ticks) on a random corner
            self.rotate_official(
                random.choice(f_keys), 
                random.randint(1, 11), 
                random.choice([True, False])
            )
            
        self.is_scrambling = False
        self.update_all()

if __name__ == "__main__":
    App().mainloop()