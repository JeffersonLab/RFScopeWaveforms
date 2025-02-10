import sys
import os

# Set up the module path to include the src directory regardless of how this code is called
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))
