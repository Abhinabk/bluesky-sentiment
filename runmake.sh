#!/usr/bin/zsh

# Tmux session name 
SESSION="run_make"
# -d = datach -s= session name
tmux new-session -d -s "$SESSION" "make produce; exec zsh"
# -t = target where to execute the command
tmux split-window -h -t "$SESSION" "make process; exec zsh"

tmux split-window -v -t "$SESSION.0" "make sentiment; exec zsh"

# Balance the alyout evenly
tmux select-layout -t "$SESSION" tiled

# opens the session as i open earlier in detached mode
tmux attach-session -t "$SESSION"
