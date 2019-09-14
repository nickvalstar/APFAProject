package com.damirvandic.sparker.blocking.core;

import com.damirvandic.sparker.blocking.schemes.BlockingScheme;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class ResultsFileWriter implements ResultsWriter {
    private static final String SEP = "\t";
    private final PrintWriter out;

    public ResultsFileWriter(File out) throws IOException {
        this.out = new PrintWriter(new FileWriter(out));
        writeHeader();
    }

    private void writeHeader() {
        out.write("bootstrap");
        out.write(SEP);
        out.write("scheme");
        out.write(SEP);
        out.write("schemeType");
        out.write(SEP);
        out.write(BlockingResult.printColumnNames(SEP));
        out.write('\n');
        out.flush();
    }

    @Override
    public void write(int bootstrapNr, BlockingScheme scheme, BlockingResult result) {
        out.write(String.valueOf(bootstrapNr));
        out.write(SEP);
        out.write(scheme.componentID());

        SchemeSummarizer sum = new SchemeSummarizer(scheme.componentID());
        out.write(SEP);
        out.write(sum.schemeType());

        out.write(SEP);
        out.write(result.printRow(SEP));
        out.write('\n');
        out.flush();
    }

    @Override
    public void close() {
        out.close();
    }

    static class SchemeSummarizer {
        private final String componentID;
        private final StringBuilder b;

        SchemeSummarizer(String componentID) {
            this.componentID = componentID;
            this.b = new StringBuilder();
        }

        private boolean checkSchemeSource(String source) {
            if (componentID.contains(source)) {
                b.append(source);
                return true;
            }
            return false;
        }

        private String schemeType() {
            if (!checkSchemeSource("t:cl.cbs")) {
                checkSchemeSource("t:cl");
            }
            if (!checkSchemeSource("t:mw.cbs")) {
                checkSchemeSource("t:mw");
            }

            checkSchemeSource("_+_");
            checkSchemeSource("_*_");

            if (!checkSchemeSource("desc:cl.cbs")) {
                checkSchemeSource("desc:cl");
            }
            if (!checkSchemeSource("desc:mw.cbs")) {
                checkSchemeSource("desc:mw");
            }

            return b.toString();
        }
    }
}
