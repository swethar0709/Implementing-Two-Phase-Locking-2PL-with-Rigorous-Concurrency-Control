class Transaction:
    # Class variable to assign incremental timestamps to transactions
    TS = 0

    def __init__(self, state="active", items_locked=None):
        # Increment the timestamp and assign it to the transaction
        Transaction.TS += 1
        self.trans_timestamp = Transaction.TS
        self.trans_state = state
        # List to keep track of items locked by the transaction
        self.items_locked = items_locked if items_locked else []

    def add_items_locked(self, item):
        # Add the given item to the list of locked items, if not already locked
        if item not in self.items_locked:
            self.items_locked.append(item)

    def remove_items_locked(self, item):
        # Remove the given item from the list of locked items, if it exists
        if item in self.items_locked:
            self.items_locked.remove(item)

    def set_state(self, state):
        # Set the state of the transaction to the given state
        self.trans_state = state

class LockTable:
    def __init__(self):
        # Transaction ID holding the write lock on the item
        self.transid_WL = None
        # List of transaction IDs holding read locks on the item
        self.transid_RL = []

    def add_transid_RL(self, tid):
        # Add the given transaction ID to the list of read-locked transactions
        if tid not in self.transid_RL:
            self.transid_RL.append(tid)

    def remove_transid_RL(self, tid):
        # Remove the given transaction ID from the list of read-locked transactions
        if tid in self.transid_RL:
            self.transid_RL.remove(tid)

    def set_transid_WL(self, tid):
        # Set the given transaction ID as the one holding the write lock
        self.transid_WL = tid

    def remove_transid_WL(self):
        # Remove the write lock by setting the transaction ID holding the write lock to None
        self.transid_WL = None

class Main:
    # Dictionary to store transaction IDs and their corresponding Transaction objects
    transMap = {}
    # Dictionary to store item names and their corresponding LockTable objects
    lockMap = {}

    def __init__(self):
        # Read the input file and store its lines as data
        self.data = self.read_file()

    def read_file(self):
        # Read lines from the "input.txt" file and return them as a list
        with open("input_4.txt", "r") as file:
            return file.readlines()

    def process_data(self):
        # Process each operation (line) in the data
        for line in self.data:
            line = line.strip()
            cmd = line[0]
            tid = int(line[1:line.index('(') if '(' in line else line.index(';')])
            item = line[line.index('(') + 1:line.index(')')] if '(' in line else None

            if cmd == 'b':
                # Begin transaction operation
                # Create a new Transaction object and add it to the transaction map
                self.transMap[tid] = Transaction()
                # Print the details of the transaction (beginning, timestamp, state)
                print(f"b{tid}; T{tid} begins. Id={tid}. TS={self.transMap[tid].trans_timestamp}. state={self.transMap[tid].trans_state}.")

            elif cmd in ['r', 'w']:
                # Read or write operation
                if item not in self.lockMap:
                    # If the item is not in the lock table, create a new LockTable entry for it
                    self.lockMap[item] = LockTable()

                # Collect existing locks (read and write) on the item
                existing_locks = [self.lockMap[item].transid_WL] + self.lockMap[item].transid_RL
                if any(self.transMap[lock].trans_timestamp < self.transMap[tid].trans_timestamp for lock in existing_locks if lock):
                    # Check for conflicts, if any transaction with a higher timestamp holds a lock
                    # Transaction is aborted due to wait-die method
                    print(f"{cmd}{tid}({item}); T{tid} is aborted due to wait-die.")
                    self.transMap[tid].set_state("aborted")
                else:
                    if cmd == 'r':
                        # Read operation
                        # Add the transaction ID to the list of read-locked transactions for the item
                        self.lockMap[item].add_transid_RL(tid)
                        # Add the locked item to the list of items locked by the transaction
                        self.transMap[tid].add_items_locked(item)
                        # Print the details of the read operation
                        print(f"r{tid}({item}); {item} is read locked by T{tid}.")

                    else:
                        # Write operation
                        if self.lockMap[item].transid_WL is None or self.lockMap[item].transid_WL == tid:
                            # If the item is not write-locked or is write-locked by the same transaction
                            # Set the transaction ID as the one holding the write lock for the item
                            self.lockMap[item].set_transid_WL(tid)
                            if tid in self.lockMap[item].transid_RL:
                                # If the transaction also holds a read lock, upgrade to a write lock
                                # Remove the transaction ID from the read-locked transactions
                                self.lockMap[item].remove_transid_RL(tid)
                                print(f"w{tid}({item}); read lock on {item} by T{tid} is upgraded to write lock.")
                            else:
                                # Print the details of the write operation
                                print(f"w{tid}({item}); {item} is write locked by T{tid}.")
                        else:
                            # If the item is write-locked by a different transaction, block the write operation
                            print(f"w{tid}({item}); Write operation blocked.")
                            self.transMap[tid].set_state("blocked")

            elif cmd == 'e':
                # End transaction operation
                if self.transMap[tid].trans_state == "aborted":
                    # If the transaction is already aborted, print the aborted message
                    print(f"e{tid}; T{tid} is already aborted.")
                else:
                    # Commit the transaction and release its locks
                    self.transMap[tid].set_state("committed")
                    print(f"e{tid}; T{tid} is committed.")
                    for item in self.transMap[tid].items_locked:
                        # Release write lock (if held) and read locks on the item
                        if self.lockMap[item].transid_WL == tid:
                            self.lockMap[item].remove_transid_WL()
                        if tid in self.lockMap[item].transid_RL:
                            self.lockMap[item].remove_transid_RL(tid)
                    # Clear the list of locked items for the transaction
                    self.transMap[tid].items_locked.clear()

        # After processing all transactions, print the final state of all transactions
        print("Final state:")
        for tid, transaction in self.transMap.items():
            print(f"T{tid}: {transaction.trans_state}.", end=" ")
        print()


    def run(self):
        # Main method to run the 2PL simulation
        print("Implementation of Rigorous 2PL")
        self.process_data()

if __name__ == "__main__":
    # Create an instance of the Main class and run the simulation
    main_obj = Main()
    main_obj.run()
