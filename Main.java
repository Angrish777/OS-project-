import java.io.*;
import java.util.*;

public class Main {

    private static File findExecutable(String command) {
        String path = System.getenv("PATH");
        String[] directories = path.split(File.pathSeparator);

        for (String dir : directories) {
            File file = new File(dir, command);
            if (file.exists() && file.isFile() && file.canExecute()) {
                return file;
            }
        }
        return null;
    }

    private static String[] parseCommand(String input) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuotes = false;
        boolean inDoubleQuotes = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (c == '\\') {
                if (i + 1 < input.length()) {
                    char next = input.charAt(i + 1);
                    if (inDoubleQuotes) {
                        if (next == '"' || next == '\\') {
                            current.append(next);
                            i++;
                            continue;
                        }
                        current.append('\\');
                        continue;
                    }
                    if (!inSingleQuotes) {
                        current.append(next);
                        i++;
                        continue;
                    }
                }
            }

            if (c == '\'' && !inDoubleQuotes) {
                inSingleQuotes = !inSingleQuotes;
            } else if (c == '"' && !inSingleQuotes) {
                inDoubleQuotes = !inDoubleQuotes;
            } else if (Character.isWhitespace(c) && !inSingleQuotes && !inDoubleQuotes) {
                if (current.length() > 0) {
                    args.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            args.add(current.toString());
        }

        return args.toArray(new String[0]);
    }

    static class Job {
        int id;
        long pid;
        String command;
        Process process;

        Job(int id, long pid, String command, Process process) {
            this.id = id;
            this.pid = pid;
            this.command = command;
            this.process = process;
        }
    }

    private static int nextJobId(List<Job> jobs) {
        Set<Integer> usedIds = new HashSet<>();
        for (Job job : jobs) {
            usedIds.add(job.id);
        }
        int id = 1;
        while (usedIds.contains(id)) {
            id++;
        }
        return id;
    }

    private static void reapJobs(List<Job> jobs) {
        List<Job> doneJobs = new ArrayList<>();

        for (Job job : jobs) {
            if (!job.process.isAlive()) {
                doneJobs.add(job);
            }
        }

        List<Job> allVisible = new ArrayList<>(jobs);

        for (Job doneJob : doneJobs) {
            int i = allVisible.indexOf(doneJob);
            char marker = ' ';
            if (i == allVisible.size() - 1) {
                marker = '+';
            } else if (i == allVisible.size() - 2) {
                marker = '-';
            }
            System.out.printf("[%d]%c  %-24s%s%n", doneJob.id, marker, "Done", doneJob.command);
        }

        jobs.removeAll(doneJobs);
    }

    private static void listJobs(List<Job> jobs) {
        List<Job> doneJobs = new ArrayList<>();

        for (Job job : jobs) {
            if (!job.process.isAlive()) {
                doneJobs.add(job);
            }
        }

        for (int i = 0; i < jobs.size(); i++) {
            Job job = jobs.get(i);
            char marker = ' ';
            if (i == jobs.size() - 1) {
                marker = '+';
            } else if (i == jobs.size() - 2) {
                marker = '-';
            }

            String status = job.process.isAlive() ? "Running" : "Done";
            String cmd = job.process.isAlive()
                ? job.command + " &"
                : job.command;

            System.out.printf("[%d]%c  %-24s%s%n", job.id, marker, status, cmd);
        }

        jobs.removeAll(doneJobs);
    }

    private static final Set<String> BUILTINS = new HashSet<>(
        Arrays.asList("echo", "exit", "type", "pwd", "cd", "jobs")
    );

    private static boolean isBuiltin(String command) {
        return BUILTINS.contains(command);
    }

    private static String runBuiltin(String[] cmd, File currentDirectory) {
        switch (cmd[0]) {
            case "echo": {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i < cmd.length; i++) {
                    if (i > 1) sb.append(" ");
                    sb.append(cmd[i]);
                }
                return sb.toString() + "\n";
            }
            case "pwd":
                return currentDirectory.getAbsolutePath() + "\n";
            case "type": {
                if (cmd.length < 2) return "\n";
                String command = cmd[1];
                if (isBuiltin(command)) {
                    return command + " is a shell builtin\n";
                }
                File executable = findExecutable(command);
                if (executable != null) {
                    return command + " is " + executable.getAbsolutePath() + "\n";
                }
                return command + ": not found\n";
            }
            default:
                return "\n";
        }
    }

    private static void pipeStreams(InputStream src, OutputStream dst) {
        Thread t = new Thread(() -> {
            try {
                byte[] buf = new byte[1];
                int b;
                while ((b = src.read(buf)) != -1) {
                    dst.write(buf, 0, b);
                    dst.flush();
                }
                dst.close();
            } catch (IOException e) {
            }
        });
        t.setDaemon(false);
        t.start();
    }

    private static void executePipeline(List<String[]> commands, File currentDirectory) throws Exception {
        int n = commands.size();

        PipedOutputStream[] pipeOuts = new PipedOutputStream[n - 1];
        PipedInputStream[] pipeIns = new PipedInputStream[n - 1];

        for (int i = 0; i < n - 1; i++) {
            pipeOuts[i] = new PipedOutputStream();
            pipeIns[i] = new PipedInputStream(pipeOuts[i], 65536);
        }

        List<Process> processes = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            String[] cmd = commands.get(i);
            boolean builtin = isBuiltin(cmd[0]);

            InputStream stageIn = (i == 0) ? null : pipeIns[i - 1];
            OutputStream stageOut = (i == n - 1) ? System.out : pipeOuts[i];

            if (builtin) {
                final String output = runBuiltin(cmd, currentDirectory);
                final OutputStream out = stageOut;
                final InputStream in = stageIn;

                Thread t = new Thread(() -> {
                    try {
                        if (in != null) {
                            in.transferTo(OutputStream.nullOutputStream());
                            in.close();
                        }
                        out.write(output.getBytes());
                        out.flush();
                        if (out != System.out) {
                            out.close();
                        }
                    } catch (IOException e) {
                    }
                });
                t.setDaemon(false);
                t.start();
                threads.add(t);

            } else {
                ProcessBuilder pb = new ProcessBuilder(cmd);
                pb.directory(currentDirectory);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);

                if (i == 0) {
                    pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
                }
                if (i == n - 1) {
                    pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                }

                Process p = pb.start();
                processes.add(p);

                if (stageIn != null) {
                    final InputStream src = stageIn;
                    final OutputStream dst = p.getOutputStream();
                    pipeStreams(src, dst);
                }

                if (i < n - 1) {
                    final InputStream src = p.getInputStream();
                    final OutputStream dst = stageOut;
                    pipeStreams(src, dst);
                }
            }
        }

        for (Thread t : threads) {
            t.join();
        }
        for (Process p : processes) {
            p.waitFor();
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        File currentDirectory = new File(System.getProperty("user.dir"));
        List<Job> jobs = new ArrayList<>();

        while (true) {
            Thread.sleep(100);
            reapJobs(jobs);
            System.out.print("$ ");
            String input = scanner.nextLine();

            String redirectFile = null;
            String errorRedirectFile = null;
            String appendFile = null;
            String errorAppendFile = null;

            if (input.contains(" 2>> ")) {
                String[] temp = input.split(" 2>> ", 2);
                input = temp[0];
                errorAppendFile = temp[1];
            } else if (input.contains(" 1>> ")) {
                String[] temp = input.split(" 1>> ", 2);
                input = temp[0];
                appendFile = temp[1];
            } else if (input.contains(" >> ")) {
                String[] temp = input.split(" >> ", 2);
                input = temp[0];
                appendFile = temp[1];
            }

            if (input.contains(" 2> ")) {
                String[] temp = input.split(" 2> ", 2);
                input = temp[0];
                errorRedirectFile = temp[1];
            }

            if (input.contains(" 1> ")) {
                String[] temp = input.split(" 1> ", 2);
                input = temp[0];
                redirectFile = temp[1];
            } else if (input.contains(" > ")) {
                String[] temp = input.split(" > ", 2);
                input = temp[0];
                redirectFile = temp[1];
            }

            if (input.contains(" | ")) {
                String[] segments = input.split(" \\| ");
                List<String[]> commands = new ArrayList<>();
                for (String segment : segments) {
                    commands.add(parseCommand(segment.trim()));
                }
                executePipeline(commands, currentDirectory);
                continue;
            }

            if (input.equals("exit") || input.equals("exit 0")) {
                break;

            } else if (input.startsWith("echo ")) {
                if (errorRedirectFile != null) {
                    new PrintWriter(errorRedirectFile).close();
                }
                if (errorAppendFile != null) {
                    new FileWriter(errorAppendFile, true).close();
                }

                String[] parts = parseCommand(input);
                StringBuilder output = new StringBuilder();
                for (int i = 1; i < parts.length; i++) {
                    if (i > 1) output.append(" ");
                    output.append(parts[i]);
                }

                if (appendFile != null) {
                    try (FileWriter fw = new FileWriter(appendFile, true);
                         PrintWriter pw = new PrintWriter(fw)) {
                        pw.println(output.toString());
                    }
                } else if (redirectFile != null) {
                    try (PrintWriter pw = new PrintWriter(redirectFile)) {
                        pw.println(output.toString());
                    }
                } else {
                    System.out.println(output.toString());
                }

            } else if (input.equals("pwd")) {
                if (errorRedirectFile != null) {
                    new PrintWriter(errorRedirectFile).close();
                }
                if (errorAppendFile != null) {
                    new FileWriter(errorAppendFile, true).close();
                }

                String result = currentDirectory.getAbsolutePath();

                if (appendFile != null) {
                    try (FileWriter fw = new FileWriter(appendFile, true);
                         PrintWriter pw = new PrintWriter(fw)) {
                        pw.println(result);
                    }
                } else if (redirectFile != null) {
                    try (PrintWriter pw = new PrintWriter(redirectFile)) {
                        pw.println(result);
                    }
                } else {
                    System.out.println(result);
                }

            } else if (input.startsWith("cd ")) {
                String path = input.substring(3).trim();
                File target;

                if (path.startsWith("~")) {
                    path = System.getenv("HOME") + path.substring(1);
                    target = new File(path);
                } else {
                    target = new File(path);
                    if (!target.isAbsolute()) {
                        target = new File(currentDirectory, path);
                    }
                }

                if (target.exists() && target.isDirectory()) {
                    currentDirectory = target.getCanonicalFile();
                } else {
                    System.out.println("cd: " + path + ": No such file or directory");
                }

            } else if (input.startsWith("type ")) {
                String command = input.substring(5);

                if (errorRedirectFile != null) {
                    new PrintWriter(errorRedirectFile).close();
                }
                if (errorAppendFile != null) {
                    new FileWriter(errorAppendFile, true).close();
                }

                if (command.equals("echo") || command.equals("exit")
                        || command.equals("type") || command.equals("pwd")
                        || command.equals("cd") || command.equals("jobs")) {

                    String result = command + " is a shell builtin";
                    if (redirectFile != null) {
                        try (PrintWriter pw = new PrintWriter(redirectFile)) {
                            pw.println(result);
                        }
                    } else {
                        System.out.println(result);
                    }

                } else {
                    File executable = findExecutable(command);
                    if (executable != null) {
                        String result = command + " is " + executable.getAbsolutePath();
                        if (redirectFile != null) {
                            try (PrintWriter pw = new PrintWriter(redirectFile)) {
                                pw.println(result);
                            }
                        } else {
                            System.out.println(result);
                        }
                    } else {
                        String result = command + ": not found";
                        if (redirectFile != null) {
                            try (PrintWriter pw = new PrintWriter(redirectFile)) {
                                pw.println(result);
                            }
                        } else {
                            System.out.println(result);
                        }
                    }
                }

            } else if (input.equals("jobs")) {
                Thread.sleep(100);
                listJobs(jobs);

            } else {
                String[] parts = parseCommand(input);

                boolean isBackground = parts.length > 0 && parts[parts.length - 1].equals("&");
                if (isBackground) {
                    parts = Arrays.copyOf(parts, parts.length - 1);
                }

                String command = parts[0];
                File executable = findExecutable(command);

                if (executable != null) {
                    ProcessBuilder pb = new ProcessBuilder(parts);
                    pb.directory(currentDirectory);

                    if (isBackground) {
                        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                        Process process = pb.start();
                        int jobId = nextJobId(jobs);
                        long pid = process.pid();
                        jobs.add(new Job(jobId, pid, String.join(" ", parts), process));
                        System.out.println("[" + jobId + "] " + pid);
                    } else {
                        if (appendFile != null) {
                            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(appendFile)));
                        } else if (redirectFile != null) {
                            pb.redirectOutput(new File(redirectFile));
                        }

                        if (errorRedirectFile != null) {
                            pb.redirectError(new File(errorRedirectFile));
                        } else if (errorAppendFile != null) {
                            pb.redirectError(ProcessBuilder.Redirect.appendTo(new File(errorAppendFile)));
                        }

                        Process process = pb.start();

                        if (redirectFile == null && appendFile == null) {
                            process.getInputStream().transferTo(System.out);
                        }
                        if (errorRedirectFile == null && errorAppendFile == null) {
                            process.getErrorStream().transferTo(System.err);
                        }

                        process.waitFor();
                    }

                } else {
                    String error = command + ": command not found";

                    if (errorAppendFile != null) {
                        try (FileWriter fw = new FileWriter(errorAppendFile, true);
                             PrintWriter pw = new PrintWriter(fw)) {
                            pw.println(error);
                        }
                    } else if (errorRedirectFile != null) {
                        try (PrintWriter pw = new PrintWriter(errorRedirectFile)) {
                            pw.println(error);
                        }
                    } else {
                        System.err.println(error);
                    }
                }
            }
        }

        scanner.close();
    }
}